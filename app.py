import os, re, time, pickle, xml.etree.ElementTree as ET, datetime, pathlib
from io import BytesIO
from urllib.parse import urljoin, urlparse

import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.parquet as pq
import requests
import streamlit as st
import tldextract
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser

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

# ───────────────────────── Main UI ─────────────────────────
st.title("SEO Crawler & Reporter 📄")
start_url = st.text_input("Website URL", placeholder="https://example.com")
max_depth = st.slider("Max crawl depth", 0, 5, 2)
start_btn = st.button("Start crawl")

STATE_FILE = "crawl_state.pkl"
HISTORY_DIR = pathlib.Path("history"); HISTORY_DIR.mkdir(exist_ok=True)

# ───────────────────────── Helpers ─────────────────────────
def polite_get(u): time.sleep(delay_sec); return requests.get(u, timeout=10, headers=HEADERS)

def is_internal(base, link):
    b = tldextract.extract(base).registered_domain
    l = tldextract.extract(link).registered_domain
    return (b == l) or (l == "")

def allowed_path(path): return (not inc_re or inc_re.search(path)) and (not exc_re or not exc_re.search(path))

def fetch_robots(root):
    rp = RobotExclusionRulesParser()
    try: rp.parse(polite_get(urljoin(root, "/robots.txt")).text.splitlines())
    except Exception: pass
    return rp

def seed_from_sitemap(root):
    try:
        xml_data = polite_get(urljoin(root, "/sitemap.xml")).content
        tree = ET.fromstring(xml_data)
        return [loc.text.strip() for loc in tree.iter("{*}loc")]
    except Exception: return []

# ───────────────────────── Crawl core ─────────────────────────
visited, rows, pages_crawled, SAVE_EVERY = set(), [], 0, 25
def save_state(): pickle.dump((visited, rows), open(STATE_FILE, "wb"))
def load_state(): return pickle.load(open(STATE_FILE, "rb")) if os.path.exists(STATE_FILE) else (set(), [])

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

        # basic
        title = soup.title.string.strip() if soup.title else ""
        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else ""

        # deeper SEO
        h_tags = " | ".join(h.get_text(strip=True) for h in soup.select("h1, h2, h3, h4, h5, h6"))
        meta_robots = (soup.find("meta", attrs={"name": "robots"}) or {}).get("content", "")
        canonical = (soup.find("link", rel="canonical") or {}).get("href", "")
        og_title = (soup.find("meta", property="og:title") or {}).get("content", "")
        og_desc = (soup.find("meta", property="og:description") or {}).get("content", "")
        tw_card = (soup.find("meta", attrs={"name": "twitter:card"}) or {}).get("content", "")
        img_alts = " | ".join(img.get("alt", "") for img in soup.find_all("img"))
        schema_types = " | ".join(sorted(
            set(e.get("type", "").replace("https://schema.org/","").replace("http://schema.org/","")
                for e in soup.find_all(attrs={"type": re.compile("schema.org")})
            )
        ))

        rows.append({
            "URL": url, "Title": title, "Meta description": description,
            "H1‑H6": h_tags, "Meta robots": meta_robots, "Canonical": canonical,
            "OG title": og_title, "OG description": og_desc, "Twitter card": tw_card,
            "Image alts": img_alts, "Schema types": schema_types
        })

        pages_crawled += 1
        if pages_crawled % SAVE_EVERY == 0: save_state()

        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if urlparse(link).scheme in ("http", "https") and is_internal(base, link):
                crawl(link, base, depth + 1, rp)
    except Exception as e:
        st.error(f"Error on {url}: {e}")

# ───────────────────────── Run crawl ─────────────────────────
if start_btn and start_url:
    base_url = start_url.strip().rstrip("/")
    if resume and os.path.exists(STATE_FILE):
        visited, rows = load_state(); st.info(f"Resumed with {len(visited)} URLs.")
    elif not resume and os.path.exists(STATE_FILE): os.remove(STATE_FILE)

    rp = fetch_robots(base_url)
    for s in seed_from_sitemap(base_url) or [base_url]:
        crawl(s, base_url, 0, rp)

    save_state()

    if not rows:
        st.warning("No pages crawled."); st.stop()

    df = pd.DataFrame(rows).drop_duplicates("URL")
    order = ["URL","Title","Meta description","H1‑H6","Meta robots","Canonical",
             "OG title","OG description","Twitter card","Image alts","Schema types"]
    df = df[order]

    # ────────── Quality flags ──────────
    df["Title empty"]     = df["Title"] == ""
    df["Title too long"]  = df["Title"].str.len() > 60
    df["Desc empty"]      = df["Meta description"] == ""
    df["Desc too long"]   = df["Meta description"].str.len() > 155
    df["Title duplicate"] = df.duplicated("Title", keep=False)

    st.success(f"✅ Finished! {len(df)} pages collected.")
    st.dataframe(df, use_container_width=True)

    issues = df[df[["Title empty","Title too long","Title duplicate","Desc empty","Desc too long"]].any(axis=1)]
    if not issues.empty:
        st.subheader("⚠️ Pages with issues")
        st.dataframe(issues, use_container_width=True)

    # ────────── Charts ──────────
    df["Depth"] = df["URL"].str.count("/") - base_url.count("/")
    depth_stats = df[df["Meta description"] == ""].groupby("Depth").size()
    fig1 = plt.figure(); depth_stats.plot(kind="bar"); plt.title("Missing Meta Descriptions by Depth"); plt.xlabel("Depth"); plt.ylabel("Pages")
    st.pyplot(fig1)

    title_quality = df["Title"].apply(lambda t: "Empty" if t == "" else ("Too long" if len(t) > 60 else "OK")).value_counts()
    fig2 = plt.figure(); title_quality.plot(kind="pie", autopct="%1.0f%%"); plt.title("Title Tag Quality"); plt.ylabel("")
    st.pyplot(fig2)

    # ────────── Export buttons ──────────
    st.download_button("Download CSV",
                       df.to_csv(index=False).encode("utf-8"),
                       "meta_data.csv","text/csv")
    st.download_button("Download JSON",
                       df.to_json(orient="records", indent=2).encode("utf-8"),
                       "meta_data.json","application/json")

    xbuf = BytesIO()
    with pd.ExcelWriter(xbuf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="SEO")

    st.download_button(
        "Download Excel",
        xbuf.getvalue(),
        "meta_data.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # ────────── Historical diff ──────────
    today_file = HISTORY_DIR / f"{datetime.date.today()}.parquet"
    df.to_parquet(today_file, index=False)
    prev_files = sorted(HISTORY_DIR.glob("*.parquet"))[-2:-1]
    if prev_files:
        prev = pq.read_table(prev_files[0]).to_pandas()
        merged = df.merge(prev, on="URL", how="outer", suffixes=("_new","_old"), indicator=True)
        changed = merged[
            (merged["_merge"] != "both") |
            (merged["Title_new"] != merged["Title_old"]) |
            (merged["Meta description_new"] != merged["Meta description_old"])
        ]
        if not changed.empty:
            st.subheader("🔄 Changes since last crawl")
            show_cols = ["URL","Title_old","Title_new","Meta description_old","Meta description_new"]
            st.dataframe(changed[show_cols], use_container_width=True)

    st.write(f"Crawl UA: `{HEADERS['User-Agent']}`")
