import os, re, time, pickle, xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import streamlit as st
import tldextract
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser

# ---------- Sidebar : Crawl settings ----------
st.sidebar.header("Crawl settings")

# User‑Agent
UA_OPTIONS = {
    "StreamlitCrawler (default)": "StreamlitCrawler/1.0 (+https://share.streamlit.io)",
    "Googlebot‑Desktop": (
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
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

# Include / Exclude regex
inc_pattern = st.sidebar.text_input("Include pattern (regex)", "")
exc_pattern = st.sidebar.text_input("Exclude pattern (regex)", "")
inc_re = re.compile(inc_pattern) if inc_pattern else None
exc_re = re.compile(exc_pattern) if exc_pattern else None

# Delay
delay_sec = st.sidebar.number_input("Delay between requests (seconds)", 0.0, 10.0, 0.5, 0.1)

# Resume
resume = st.sidebar.checkbox("Resume previous crawl if found", value=True)

# ---------- Streamlit main UI ----------
st.title("Meta Title & Description Crawler 📄")
start_url = st.text_input("Website URL", placeholder="https://example.com")
max_depth = st.slider("Max crawl depth", 0, 5, 2)
start_btn = st.button("Start crawl")

STATE_FILE = "crawl_state.pkl"

# ---------- Helper functions ----------
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

# ---------- Crawl core ----------
visited, rows = set(), []
pages_crawled = 0
SAVE_EVERY = 25

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
        title = soup.title.string.strip() if soup.title else ""
        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else ""
        rows.append({"URL": url, "Title": title, "Meta description": description})
        pages_crawled += 1
        if pages_crawled % SAVE_EVERY == 0:
            save_state()

        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if (
                urlparse(link).scheme in ("http", "https")
                and is_internal(base, link)
            ):
                crawl(link, base, depth + 1, rp)
    except Exception as e:
        st.error(f"Error on {url}: {e}")

# ---------- Run crawl ----------
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
        prog.progress(min(1.0, pages_crawled / 500))  # rough progress

    save_state()  # final save

    if rows:
        df = pd.DataFrame(rows).drop_duplicates("URL")
        st.success(f"✅ Finished! {len(df)} pages collected.")
        st.dataframe(df, use_container_width=True)
        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"),
                           "meta_data.csv", "text/csv")
    else:
        st.warning("No pages crawled.")

    st.write(f"Crawl headers used: `{HEADERS['User-Agent']}`")
