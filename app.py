# ─────────────────────────────────────────────────────────────
# SEO Crawler & Reporter – Streamlit (with live progress bar)
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
# Email input (above website URL and button)
email = st.text_input("Your Email Address", placeholder="name@example.com")
email_valid = re.match(r"^[^@]+@[^@]+\.[^@]+$", email or "")
# Optional: Show a warning if button is pressed without a valid email
if not email_valid and start_btn:
    st.warning("Please enter a valid email address to start the crawl.")
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
# ───────────────────────── Crawl containers ─────────────────────────
visited, pages_crawled, SAVE_EVERY = set(), 0, 50
rows, broken_links, image_rows = [], [], []
out_links, in_links, duplicate_map, canon_map = {}, {}, {}, {}

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
        if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type",""):
            return
        soup = BeautifulSoup(r.text, "html.parser")

        # meta + deeper SEO
        title = soup.title.string.strip() if soup.title else ""
        description = (soup.find("meta", attrs={"name": "description"}) or {})\
                          .get("content", "").strip()
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

        # links & images
        links_here = set()
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"].split("#")[0])
            if link.startswith(("mailto:", "tel:", "javascript:")): continue
            links_here.add(link)
            out_links.setdefault(url, set()).add(link)
            in_links.setdefault(link, 0); in_links[link] += 1
        for img in soup.find_all("img", src=True):
            img_url = urljoin(url, img["src"])
            image_rows.append({"Page": url, "Image": img_url,
                               "Alt": img.get("alt",""),
                               "Width": img.get("width",""),
                               "Height": img.get("height","")})

        # ── progress update ─────────────────────
        if max_pages:
            pct = min(pages_crawled / max_pages, 1.0)
            progress_bar.progress(pct)
            status_txt.text(f"Crawled {pages_crawled} / {max_pages} pages")
        else:
            progress_bar.progress((pages_crawled % 100) / 100)
            status_txt.text(f"Crawled {pages_crawled} pages…")
        # ────────────────────────────────────────

        pages_crawled += 1
        if max_pages and pages_crawled >= max_pages: return
        if pages_crawled % SAVE_EVERY == 0: save_state()

        for link in links_here:
            if urlparse(link).scheme in ("http","https") and is_internal(base, link):
                crawl(link, base, depth+1, rp)
    except Exception as e:
        st.error(f"Error on {url}: {e}")
# ───────────────── Async audits: broken links & images ─────────────────
async def fetch_head(session, url):
    try:
        r = await session.head(url, follow_redirects=True, timeout=10)
        return url, r.status_code
    except Exception:
        return url, None

async def audit_links_and_images():
    hrefs = {t for targets in out_links.values() for t in targets}
    imgs  = {row["Image"] for row in image_rows}
    async with httpx.AsyncClient(headers=HEADERS) as s:
        link_results = await asyncio.gather(*[fetch_head(s,u) for u in hrefs])
        img_results  = await asyncio.gather(*[fetch_head(s,u) for u in imgs])

    status_map = dict(link_results); img_status = dict(img_results)
    for src, targets in out_links.items():
        for t in targets:
            code = status_map.get(t); 
            if code and code >= 400:
                broken_links.append({"Source": src,"Href": t,"Status": code,
                                     "Type": "internal" if is_internal(start_url,t) else "external"})
    for row in image_rows:
        row["Status"] = img_status.get(row["Image"])

# ───────────────────────── Run crawl ─────────────────────────
if start_btn and start_url and email_valid:
    # You may want to store/log emails, or use them later
    # For example: st.info(f"Crawling as: {email}")
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

    progress_bar.progress(1.0)
    status_txt.text(f"Finished crawl – {pages_crawled} pages.")

    if not rows:
        st.warning("No pages crawled."); st.stop()

    df = pd.DataFrame(rows).drop_duplicates("URL")
    order = ["URL","Title","Meta description","H1‑H6","Meta robots","Canonical",
             "OG title","OG description","Twitter card","Schema types"]
    df = df[order]

    # quality flags
    df["Title empty"]     = df["Title"] == ""
    df["Title too long"]  = df["Title"].str.len() > 60
    df["Desc empty"]      = df["Meta description"] == ""
    df["Desc too long"]   = df["Meta description"].str.len() > 155
    df["Title duplicate"] = df.duplicated("Title", keep=False)
    # indexability overlay
    def idx(row):
        if "noindex" in row["Meta robots"].lower(): return "Noindex"
        if row["Canonical"] and row["Canonical"] != row["URL"]: return "Canonicalized"
        return "Indexable"
    df["Indexability"] = df.apply(idx, axis=1)

    dup_df = pd.DataFrame(
        [{"Cluster": k[:60]+"…" if len(k)>60 else k, "URLs": " | ".join(v)}
         for k, v in duplicate_map.items() if len(v)>1]
    )
    can_df = pd.DataFrame(
        [{"Page": s, "Canonical target": t, "Issue": (
            "Loop" if canon_map.get(t)==s else "Target not crawled")}
         for s,t in canon_map.items() if t and t!=s and
           (canon_map.get(t)==s or t not in in_links)]
    )
    broken_df = pd.DataFrame(broken_links)
    image_df  = pd.DataFrame(image_rows)
    orphan_df = pd.DataFrame([{"URL": u} for u in df["URL"] if in_links.get(u,0)==0])

    # ────────── UI tabs ──────────
    tabs = st.tabs(["All pages","Issues","Broken links","Duplicates","Canonicals",
                    "Internal graph","Images","Orphans"])
    with tabs[0]: st.dataframe(df, use_container_width=True)
    with tabs[1]:
        iss = df[(df["Title empty"]|df["Title too long"]|df["Title duplicate"]|
                  df["Desc empty"]|df["Desc too long"]|(df["Indexability"]!="Indexable"))]
        st.dataframe(iss, use_container_width=True)
    with tabs[2]: st.dataframe(broken_df, use_container_width=True)
    with tabs[3]: st.dataframe(dup_df, use_container_width=True)
    with tabs[4]: st.dataframe(can_df, use_container_width=True)
    with tabs[5]:
        dot = Digraph()
        for src, targets in list(out_links.items())[:200]:
            for t in list(targets)[:50]:
                if is_internal(base_url, t): dot.edge(src, t)
        st.graphviz_chart(dot)
    with tabs[6]: st.dataframe(image_df, use_container_width=True)
    with tabs[7]: st.dataframe(orphan_df, use_container_width=True)

    # chart
    df["Depth"] = df["URL"].str.count("/") - base_url.count("/")
    fig1 = plt.figure(); df[df["Meta description"]==""].groupby("Depth").size().plot(kind="bar")
    plt.title("Missing Meta Descriptions by Depth"); plt.xlabel("Depth"); plt.ylabel("Pages")
    st.pyplot(fig1)

    # exports
    st.download_button("CSV", df.to_csv(index=False).encode("utf-8"), "crawl.csv","text/csv")
    st.download_button("JSON", df.to_json(orient="records",indent=2).encode("utf-8"),
                       "crawl.json","application/json")
    xbuf = BytesIO()
    with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
        df.to_excel(w,index=False,sheet_name="Pages")
        broken_df.to_excel(w,index=False,sheet_name="Broken")
        dup_df.to_excel(w,index=False,sheet_name="Duplicates")
        can_df.to_excel(w,index=False,sheet_name="Canonicals")
    st.download_button("Excel", xbuf.getvalue(), "crawl.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # historical diff
    today_file = HISTORY_DIR / f"{datetime.date.today()}.parquet"
    df.to_parquet(today_file, index=False)
    prev = sorted(HISTORY_DIR.glob("*.parquet"))[-2:-1]
    if prev:
        old = pq.read_table(prev[0]).to_pandas()
        diff = df.merge(old,on="URL",how="outer",suffixes=("_new","_old"),indicator=True)
        changed = diff[(diff["_merge"]!="both") |
                       (diff["Title_new"]!=diff["Title_old"]) |
                       (diff["Meta description_new"]!=diff["Meta description_old"])]
        if not changed.empty:
            st.subheader("🔄 Changes since last crawl")
            st.dataframe(changed[["URL","Title_old","Title_new",
                                  "Meta description_old","Meta description_new"]],
                         use_container_width=True)

    st.caption(f"Crawl UA: `{HEADERS['User-Agent']}`")
