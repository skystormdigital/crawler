import os, re, time, pickle, xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import streamlit as st
import tldextract
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser

# ───────────────── Sidebar : crawl controls ─────────────────
st.sidebar.header("Crawl settings")

UA_OPTIONS = {
    "StreamlitCrawler (default)": "StreamlitCrawler/1.0 (+https://share.streamlit.io)",
    "Googlebot‑Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot‑Mobile": (
        "Mozilla/5.0 (Linux; Android 10; Pixel 3 XL) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36 "
        "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Custom…": "",
}
ua_choice = st.sidebar.selectbox("User‑Agent", list(UA_OPTIONS.keys()))
if ua_choice == "Custom…":
    UA_OPTIONS["Custom…"] = st.sidebar.text_input("Enter custom UA string", "")
HEADERS = {"User-Agent": UA_OPTIONS[ua_choice] or UA_OPTIONS["StreamlitCrawler (default)"]}

inc_pattern = st.sidebar.text_input("Include pattern (regex)", "")
exc_pattern = st.sidebar.text_input("Exclude pattern (regex)", "")
inc_re = re.compile(inc_pattern) if inc_pattern else None
exc_re = re.compile(exc_pattern) if exc_pattern else None

delay_sec = st.sidebar.number_input("Delay between requests (s)", 0.0, 10.0, 0.5, 0.1)
resume = st.sidebar.checkbox("Resume previous crawl if found", value=True)

# ───────────────── Main UI ─────────────────
st.title("Advanced SEO Crawler 📄")
start_url = st.text_input("Website URL", placeholder="https://example.com")
max_depth = st.slider("Max crawl depth", 0, 5, 2)
start_btn = st.button("Start crawl")

STATE_FILE = "crawl_state.pkl"

# ───────────────── Helpers ─────────────────
def polite_get(url: str):
    time.sleep(delay_sec)
    return requests.get(url, timeout=10, headers=HEADERS)

def is_internal(base: str, link: str) -> bool:
    b = tldextract.extract(base).registered_domain
    l = tldextract.extract(link).registered_domain
    return (b == l) or (l == "")

def allowed_path(path: str) -> bool:
    if inc_re and not inc_re.search(path):
        return False
    if exc_re and exc_re.search(path):
        return False
    return True

def fetch_robots(root: str):
    rp = RobotExclusionRulesParser()
    try:
        data = polite_get(urljoin(root, "/robots.txt")).text
        rp.parse(data.splitlines())
    except Exception:
        pass
    return rp

def seed_from_sitemap(root: str):
    seeds = []
    try:
        xml_data = polite_get(urljoin(root, "/sitemap.xml")).content
        tree = ET.fromstring(xml_data)
        for loc in tree.iter("{*}loc"):
            seeds.append(loc.text.strip())
    except Exception:
        pass
    return seeds

# ───────────────── Crawl core ─────────────────
visited, rows = set(), []
pages_crawled, SAVE_EVERY = 0, 25

def save_state():
    with open(STATE_FILE, "wb") as f:
        pickle.dump((visited, rows), f)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "rb") as f:
            return pickle.load(f)
    return set(), []

def crawl(url: str, base: str, depth: int, rp: RobotExclusionRulesParser):
    global pages_crawled
    if url in visited or depth > max_depth:
        return
    if not rp.is_allowed(HEADERS["User-Agent"], url):
        return
    parsed = urlparse(url)
    if not allowed_path(parsed.path):
        return

    visited.add(url)
    try:
        r = polite_get(url)
        if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type", ""):
            return
        soup = BeautifulSoup(r.text, "html.parser")

        # ─── basic meta ───
        title = soup.title.string.strip() if soup.title else ""
        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else ""

        # ─── deeper SEO fields ───
        h_tags = " | ".join(h.get_text(strip=True) for h in soup.select("h1, h2, h3, h4, h5, h6"))
        meta_robots = (soup.find("meta", attrs={"name": "robots"}) or {}).get("content", "")
        canonical = (soup.find("link", rel="canonical") or {}).get("href", "")
        og_title = (soup.find("meta", property="og:title") or {}).get("content", "")
        og_desc = (soup.find("meta", property="og:description") or {}).get("content", "")
        tw_card = (soup.find("meta", attrs={"name": "twitter:card"}) or {}).get("content", "")
        img_alts = " | ".join(img.get("alt", "") for img in soup.find_all("img"))
        schema_types = " | ".join(
            sorted(
                set(
                    e.get("type", "")
                    .replace("https://schema.org/", "")
                    .replace("http://schema.org/", "")
                    for e in soup.find_all(attrs={"type": re.compile("schema.org")})
                )
            )
        )

        rows.append(
            {
                "URL": url,
                "Title": title,
                "Meta description": description,
                "H1‑H6": h_tags,
                "Meta robots": meta_robots,
                "Canonical": canonical,
                "OG title": og_title,
                "OG description": og_desc,
                "Twitter card": tw_card,
                "Image alts": img_alts,
                "Schema types": schema_types,
            }
        )

        pages_crawled += 1
        if pages_crawled % SAVE_EVERY == 0:
            save_state()

        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if urlparse(link).scheme in ("http", "https") and is_internal(base, link):
                crawl(link, base, depth + 1, rp)
    except Exception as e:
        st.error(f"Error on {url}: {e}")

# ───────────────── Run ─────────────────
if start_btn and start_url:
    base_url = start_url.strip()
    if resume and os.path.exists(STATE_FILE):
        visited, rows = load_state()
        st.info(f"Resumed with {len(visited)} URLs already visited.")
    elif not resume and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    rp = fetch_robots(base_url)
    seeds = seed_from_sitemap(base_url) or [base_url]

    prog = st.progress(0.0, "Crawling…")
    for s in seeds:
        crawl(s, base_url, 0, rp)
        prog.progress(min(1.0, pages_crawled / 500))
    save_state()

    if rows:
        df = pd.DataFrame(rows).drop_duplicates("URL")
        order = [
            "URL",
            "Title",
            "Meta description",
            "H1‑H6",
            "Meta robots",
            "Canonical",
            "OG title",
            "OG description",
            "Twitter card",
            "Image alts",
            "Schema types",
        ]
        df = df[[c for c in order if c in df.columns]]

        st.success(f"✅ Finished! {len(df)} pages collected.")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Download CSV",
            df.to_csv(index=False).encode("utf-8"),
            "meta_data.csv",
            "text/csv",
        )
    else:
        st.warning("No pages crawled.")

    st.write(f"Crawl headers used: `{HEADERS['User-Agent']}`")
