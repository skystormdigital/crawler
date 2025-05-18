# ─────────────────────────────────────────────────────────────
# SEO Crawler & Reporter – all‑in‑one Streamlit app
# (now with crawl‑progress bar)
# ─────────────────────────────────────────────────────────────

import os, re, time, pickle, xml.etree.ElementTree as ET, datetime, pathlib, asyncio
from io import BytesIO
from urllib.parse import urljoin, urlparse

import httpx
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.parquet as pq
import requests
import streamlit as st
import tldextract
from bs4 import BeautifulSoup
from graphviz import Digraph
from robotexclusionrulesparser import RobotExclusionRulesParser

st.set_page_config(page_title="SEO Crawler", layout="wide")

# ───────────────────────── Sidebar controls ─────────────────────────
st.sidebar.header("Crawl settings")

UA_OPTS = {
    "StreamlitCrawler (default)": "StreamlitCrawler/1.0 (+https://share.streamlit.io)",
    "Googlebot‑Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot‑Mobile": (
        "Mozilla/5.0 (Linux; Android 10; Pixel 3 XL) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36 "
        "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Custom…": "",
}
ua_choice = st.sidebar.selectbox("User‑Agent", list(UA_OPTS.keys()))
if ua_choice == "Custom…":
    UA_OPTS["Custom…"] = st.sidebar.text_input("Enter custom UA string", "")
HEADERS = {"User-Agent": UA_OPTS[ua_choice] or UA_OPTS["StreamlitCrawler (default)"]}

inc_pattern = st.sidebar.text_input("Include pattern (regex)", "")
exc_pattern = st.sidebar.text_input("Exclude pattern (regex)", "")
inc_re = re.compile(inc_pattern) if inc_pattern else None
exc_re = re.compile(exc_pattern) if exc_pattern else None

delay_sec = st.sidebar.number_input("Delay between requests (s)", 0.0, 10.0, 0.5, 0.1)
resume = st.sidebar.checkbox("Resume previous crawl", True)
max_depth = st.sidebar.slider("Max depth", 0, 6, 2)
max_pages = st.sidebar.number_input("Stop after N pages (0 = unlimited)", 0, 100000, 0, 100)

# ───────────────────────── Main UI ─────────────────────────
st.title("SEO Crawler & Reporter 📄")
start_url = st.text_input("Website URL", placeholder="https://example.com")
start_btn = st.button("Start crawl")

# progress bar placeholders
progress_bar = st.empty()
status_txt   = st.empty()

STATE_FILE = "crawl_state.pkl"
HISTORY_DIR = pathlib.Path("history"); HISTORY_DIR.mkdir(exist_ok=True)

# ───────────────────────── Helpers ─────────────────────────
def polite_get(u):
    time.sleep(delay_sec)
    return requests.get(u, timeout=10, headers=HEADERS)

def is_internal(base, link):
    b = tldextract.extract(base).registered_domain
    l = tldextract.extract(link).registered_domain
    return (b == l) or (l == "")

def allowed_path(path):
    return (not inc_re or inc_re.search(path)) and (not exc_re or not exc_re.search(path))

def fetch_robots(root):
    rp = RobotExclusionRulesParser()
    try:
        rp.parse(polite_get(urljoin(root, "/robots.txt")).text.splitlines())
    except Exception:
        pass
    return rp

def seed_from_sitemap(root):
    try:
        xml_data = polite_get(urljoin(root, "/sitemap.xml")).content
        tree = ET.fromstring(xml_data)
        return [loc.text.strip() for loc in tree.iter("{*}loc")]
    except Exception:
        return []
# ───────────────────────── Crawl data containers ─────────────────────────
visited, pages_crawled, SAVE_EVERY = set(), 0, 50
rows = []                             # page‑level SEO data
broken_links = []                     # src → href with bad status
out_links, in_links = {}, {}          # for internal‑link graph
duplicate_map = {}                    # (title+desc) → [urls]
canon_map = {}                        # url → canonical target
image_rows = []                       # heavy / dimensionless images

def save_state():
    pickle.dump((visited, rows, broken_links, out_links, in_links,
                 duplicate_map, canon_map, image_rows),
                open(STATE_FILE, "wb"))

def load_state():
    if os.path.exists(STATE_FILE):
        return pickle.load(open(STATE_FILE, "rb"))
    return (set(), [], [], {}, {}, {}, {}, [])

# ───────────────────────── Crawl core ─────────────────────────
def crawl(url, base, depth, rp):
    global pages_crawled
    if url in visited or depth > max_depth: return
    if not rp.is_allowed(HEADERS["User-Agent"], url): return
    if not allowed_path(urlparse(url).path): return
    visited.add(url)

    try:
        r = polite_get(url)
        if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type", ""): return
        soup = BeautifulSoup(r.text, "html.parser")

        # basic meta
        title = soup.title.string.strip() if soup.title else ""
        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else ""

        # deeper SEO
        h_tags = " | ".join(h.get_text(strip=True) for h in soup.select("h1, h2, h3, h4, h5, h6")[:20])
        meta_robots = (soup.find("meta", attrs={"name": "robots"}) or {}).get("content", "")
        canonical = (soup.find("link", rel="canonical") or {}).get("href", "")
        og_title = (soup.find("meta", property="og:title") or {}).get("content", "")
        og_desc  = (soup.find("meta", property="og:description") or {}).get("content", "")
        tw_card  = (soup.find("meta", attrs={"name": "twitter:card"}) or {}).get("content", "")
        schema_types = " | ".join(sorted(
            {e.get("type","").split("/")[-1] for e in soup.find_all(attrs={"type": re.compile("schema.org")}) if e.get("type")}
        ))

        rows.append({
            "URL": url, "Title": title, "Meta description": description,
            "H1‑H6": h_tags, "Meta robots": meta_robots, "Canonical": canonical,
            "OG title": og_title, "OG description": og_desc, "Twitter card": tw_card,
            "Schema types": schema_types
        })

        canon_map[url] = canonical
        dup_key = (title + description).lower().strip()
        duplicate_map.setdefault(dup_key, []).append(url)

        # link processing
        links_here = set()
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"].split("#")[0])
            if link.startswith(("mailto:", "tel:", "javascript:")): continue
            links_here.add(link)
            out_links.setdefault(url, set()).add(link)
            in_links.setdefault(link, 0)
            in_links[link] += 1
        # images
        for img in soup.find_all("img", src=True):
            img_url = urljoin(url, img["src"])
            image_rows.append({"Page": url, "Image": img_url,
                               "Alt": img.get("alt",""),
                               "Width": img.get("width",""), "Height": img.get("height","")})

        pages_crawled += 1
        if pages_crawled % SAVE_EVERY == 0: save_state()

        for link in links_here:
            if urlparse(link).scheme in ("http","https") and is_internal(base, link):
                crawl(link, base, depth+1, rp)
    except Exception as e:
        st.error(f"Error on {url}: {e}")
# ───────────────────────── Async checks: broken links & images ─────────────────────────
async def fetch_head(session, url):
    try:
        r = await session.head(url, follow_redirects=True, timeout=10)
        return url, r.status_code
    except Exception:
        return url, None

async def audit_links_and_images():
    # collect unique href targets + unique image URLs
    hrefs = {t for targets in out_links.values() for t in targets}
    imgs  = {row["Image"] for row in image_rows}
    async with httpx.AsyncClient(headers=HEADERS) as session:
        link_tasks = [fetch_head(session, u) for u in hrefs]
        img_tasks  = [fetch_head(session, u) for u in imgs]
        link_results = await asyncio.gather(*link_tasks)
        img_results  = await asyncio.gather(*img_tasks)

    status_map = dict(link_results)
    img_status = dict(img_results)

    # broken links table
    for src, targets in out_links.items():
        for t in targets:
            code = status_map.get(t)
            if code and code >= 400:
                broken_links.append({
                    "Source": src, "Href": t, "Status": code,
                    "Type": "internal" if is_internal(start_url, t) else "external"
                })

    # enrich image rows
    for row in image_rows:
        url = row["Image"]
        code = img_status.get(url)
        row["Status"] = code
        row["KB"] = ""
        if code and code < 400:
            try:
                size = int(asyncio.run(fetch_head(session, url))[1].headers.get("Content-Length", 0))
                row["KB"] = round(size/1024,1)
            except Exception:
                pass

# ───────────────────────── Run crawl ─────────────────────────
if start_btn and start_url:
    base_url = start_url.strip().rstrip("/")
    if resume and os.path.exists(STATE_FILE):
        (visited, rows, broken_links, out_links, in_links,
         duplicate_map, canon_map, image_rows) = load_state()
        st.info(f"Resumed with {len(visited)} URLs.")
    elif not resume and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    rp = fetch_robots(base_url)
    for s in seed_from_sitemap(base_url) or [base_url]:
        crawl(s, base_url, 0, rp)

    asyncio.run(audit_links_and_images())
    save_state()

    if not rows:
        st.warning("No pages crawled."); st.stop()

    df = pd.DataFrame(rows).drop_duplicates("URL")
    order = ["URL","Title","Meta description","H1‑H6","Meta robots","Canonical",
             "OG title","OG description","Twitter card","Schema types"]
    df = df[order]

    # quality
    df["Title empty"]     = df["Title"] == ""
    df["Title too long"]  = df["Title"].str.len() > 60
    df["Desc empty"]      = df["Meta description"] == ""
    df["Desc too long"]   = df["Meta description"].str.len() > 155
    df["Title duplicate"] = df.duplicated("Title", keep=False)
    # indexability overlay
    def index_status(row):
        if row["Meta robots"].lower().find("noindex") != -1:
            return "Noindex"
        if row["Canonical"] and row["Canonical"] != row["URL"]:
            return "Canonicalized"
        return "Indexable"
    df["Indexability"] = df.apply(index_status, axis=1)

    # duplicate clusters table
    dup_clusters = [
        {"Cluster key": k[:60]+"…" if len(k)>60 else k, "URLs": " | ".join(v)}
        for k, v in duplicate_map.items() if len(v) > 1
    ]
    dup_df = pd.DataFrame(dup_clusters)

    # canonical issues
    can_issues = []
    for src, tgt in canon_map.items():
        if not tgt: continue
        if tgt == src: continue
        if tgt in canon_map and canon_map[tgt] == src:
            can_issues.append({"Page": src, "Canonical target": tgt, "Issue": "Loop"})
        elif tgt not in in_links:
            can_issues.append({"Page": src, "Canonical target": tgt, "Issue": "Points to non‑crawled"})
    can_df = pd.DataFrame(can_issues)

    # broken links df
    broken_df = pd.DataFrame(broken_links)
    image_df  = pd.DataFrame(image_rows)
    # orphan pages
    orphan_df = pd.DataFrame(
        [{"URL": u, "Inbound links": in_links.get(u,0)} for u in df["URL"] if in_links.get(u,0)==0]
    )

    # ────────── UI tabs ──────────
    tabs = st.tabs(["All pages","Issues","Broken links","Duplicates","Canonicals",
                    "Internal graph","Images","Orphans"])
    with tabs[0]:
        st.dataframe(df, use_container_width=True)
    with tabs[1]:
        issues = df[df[["Title empty","Title too long","Title duplicate",
                        "Desc empty","Desc too long","Indexability"]]
                     .apply(lambda r: r["Title empty"] or r["Title too long"] or
                                      r["Title duplicate"] or r["Desc empty"] or
                                      r["Desc too long"] or r["Indexability"]!="Indexable", axis=1)]
        st.dataframe(issues, use_container_width=True)
    with tabs[2]:
        st.dataframe(broken_df, use_container_width=True)
    with tabs[3]:
        st.dataframe(dup_df, use_container_width=True)
    with tabs[4]:
        st.dataframe(can_df, use_container_width=True)
    with tabs[5]:
        dot = Digraph()
        for src, targets in list(out_links.items())[:200]:  # limit for readability
            for t in list(targets)[:50]:
                if is_internal(base_url, t):
                    dot.edge(src, t)
        st.graphviz_chart(dot)
    with tabs[6]:
        st.dataframe(image_df, use_container_width=True)
    with tabs[7]:
        st.dataframe(orphan_df, use_container_width=True)

    # charts
    df["Depth"] = df["URL"].str.count("/") - base_url.count("/")
    fig1 = plt.figure(); df[df["Meta description"]==""].groupby("Depth").size().plot(kind="bar")
    plt.title("Missing Meta Descriptions by Depth"); plt.xlabel("Depth"); plt.ylabel("Pages")
    st.pyplot(fig1)

    # exports
    st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"),
                       "crawl_data.csv","text/csv")
    st.download_button("Download JSON",
                       df.to_json(orient="records", indent=2).encode("utf-8"),
                       "crawl_data.json","application/json")
    xbuf = BytesIO()
    with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Pages")
        broken_df.to_excel(w, index=False, sheet_name="Broken")
        dup_df.to_excel(w, index=False, sheet_name="Duplicates")
        can_df.to_excel(w, index=False, sheet_name="Canonicals")
    st.download_button("Download Excel",
                       xbuf.getvalue(),"crawl_data.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # historical diff
    today_file = HISTORY_DIR / f"{datetime.date.today()}.parquet"
    df.to_parquet(today_file, index=False)
    prev_files = sorted(HISTORY_DIR.glob("*.parquet"))[-2:-1]
    if prev_files:
        prev = pq.read_table(prev_files[0]).to_pandas()
        merged = df.merge(prev, on="URL", how="outer",
                          suffixes=("_new","_old"), indicator=True)
        changed = merged[
            (merged["_merge"] != "both") |
            (merged["Title_new"] != merged["Title_old"]) |
            (merged["Meta description_new"] != merged["Meta description_old"])
        ]
        if not changed.empty:
            st.subheader("🔄 Changes since last crawl")
            st.dataframe(changed[["URL","Title_old","Title_new",
                                  "Meta description_old","Meta description_new"]],
                         use_container_width=True)

    st.write(f"Crawl UA: `{HEADERS['User-Agent']}`")
