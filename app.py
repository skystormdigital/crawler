# ─────────────────────────────────────────────────────────────
# SEO Crawler & Reporter – Streamlit  v1.3  (regex‑fix + fetch‑debug)
# ─────────────────────────────────────────────────────────────
import os, re, time, pickle, xml.etree.ElementTree as ET, datetime, pathlib, asyncio, ssl
from io import BytesIO
from urllib.parse import urljoin, urlparse

import httpx, requests, smtplib
from email.message import EmailMessage
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
import tldextract
from bs4 import BeautifulSoup
from graphviz import Digraph
from robotexclusionrulesparser import RobotExclusionRulesParser

st.set_page_config(page_title="SEO Crawler", layout="wide")

# ────────────── Sidebar settings ──────────────
st.sidebar.header("Crawl settings")

UA_OPTS = {
    "StreamlitCrawler (default)": "StreamlitCrawler/1.0 (+https://share.streamlit.io)",
    "Googlebot‑Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot‑Mobile": ("Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36 "
                         "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"),
    "Custom…": "",
}
ua_choice = st.sidebar.selectbox("User‑Agent", list(UA_OPTS.keys()))
if ua_choice == "Custom…":
    UA_OPTS["Custom…"] = st.sidebar.text_input("Enter custom UA string", "")

HEADERS = {"User-Agent": UA_OPTS[ua_choice] or UA_OPTS["StreamlitCrawler (default)"]}

# include / exclude regex – compile only if box not empty
inc_pat = st.sidebar.text_input("Include pattern (regex)", "")
exc_pat = st.sidebar.text_input("Exclude pattern (regex)", "")
inc_re  = re.compile(inc_pat) if inc_pat else None
exc_re  = re.compile(exc_pat) if exc_pat else None

delay_sec  = st.sidebar.number_input("Delay between requests (s)", 0.0, 10.0, 0.5, 0.1)
resume     = st.sidebar.checkbox("Resume previous crawl", True)
max_depth  = st.sidebar.slider("Max depth", 0, 6, 2)
max_pages  = st.sidebar.number_input("Stop after N pages (0 = unlimited)", 0, 100000, 0, 100)

# ────────────── Main UI ──────────────
st.title("SEO Crawler & Reporter 📄")
user_email = st.text_input("Email to receive the report", placeholder="you@example.com")
start_url  = st.text_input("Website URL", placeholder="https://example.com")
start_btn  = st.button("Start crawl")

progress_bar, status_txt = st.empty(), st.empty()

STATE_FILE  = "crawl_state.pkl"
HISTORY_DIR = pathlib.Path("history"); HISTORY_DIR.mkdir(exist_ok=True)

# ────────────── Helpers ──────────────
def polite_get(u):
    time.sleep(delay_sec)
    return requests.get(u, timeout=10, headers=HEADERS, allow_redirects=True)

def is_internal(base, link):
    return tldextract.extract(base).registered_domain == tldextract.extract(link).registered_domain

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
        xml = polite_get(urljoin(root, "/sitemap.xml")).content
        return [loc.text.strip() for loc in ET.fromstring(xml).iter("{*}loc")]
    except Exception:
        return []

def send_email_smtp(to_addr, subject, body, files):
    cfg = st.secrets["smtp"]
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, cfg["user"], to_addr
    msg.set_content(body)
    for fname, data, mime in files:
        maintype, subtype = mime.split("/")
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)
    ctx = ssl.create_default_context()
    with smtplib.SMTP(cfg["server"], cfg["port"]) as s:
        s.starttls(context=ctx)
        s.login(cfg["user"], cfg["password"])
        s.send_message(msg)

# ────────────── Containers ──────────────
visited, pages_crawled, SAVE_EVERY = set(), 0, 50
rows, broken_links, image_rows = [], [], []
out_links, in_links, duplicate_map, canon_map = {}, {}, {}, {}

def save_state():
    pickle.dump((visited, rows, broken_links, out_links, in_links,
                 duplicate_map, canon_map, image_rows), open(STATE_FILE, "wb"))

def load_state():
    if os.path.exists(STATE_FILE):
        return pickle.load(open(STATE_FILE, "rb"))
    return (set(), [], [], {}, {}, {}, {}, [])

# ────────────── Crawler ──────────────
def crawl(url, base, depth, rp):
    global pages_crawled
    # DEBUG lines
    st.write("🔍 trying:", url)
    if url in visited:      st.write(" ↳ skipped (visited)");  return
    if depth > max_depth:   st.write(" ↳ skipped (depth)");    return
    if not rp.is_allowed(HEADERS["User-Agent"], url):
        st.write(" ↳ blocked by robots.txt");                 return
    if not allowed_path(urlparse(url).path):
        st.write(" ↳ filtered by regex");                     return
    st.write(" ✔ fetching…")

    visited.add(url)
    try:
        r = polite_get(url)
        # DEBUG status / content‑type
        st.write(f" ↳ status {r.status_code}, content‑type {r.headers.get('Content-Type','')}")
        if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type", ""):
            return

        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string.strip() if soup.title else ""
        desc  = (soup.find("meta", {"name": "description"}) or {}).get("content", "").strip()
        htags = " | ".join(h.get_text(strip=True) for h in soup.select("h1,h2,h3,h4,h5,h6")[:20])
        meta_robots = (soup.find("meta", {"name": "robots"}) or {}).get("content", "")
        canonical   = (soup.find("link", rel="canonical") or {}).get("href", "")
        rows.append({"URL": url, "Title": title, "Meta description": desc,
                     "Headings": htags, "Meta robots": meta_robots,
                     "Canonical": canonical})
        pages_crawled += 1

        # update progress
        if max_pages:
            progress_bar.progress(min(pages_crawled / max_pages, 1.0))
        else:
            progress_bar.progress((pages_crawled % 100) / 100)
        status_txt.text(f"Crawled {pages_crawled} pages…")

        # enqueue internal links
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"].split("#")[0])
            if link.startswith(("http://", "https://")) and is_internal(base, link):
                crawl(link, base, depth + 1, rp)

    except Exception as e:
        st.error(f"{url} → {e}")

# ────────────── Run button ──────────────
if start_btn and start_url:
    base_url = start_url.strip().rstrip("/")
    if resume and os.path.exists(STATE_FILE):
        (visited, rows, broken_links, out_links, in_links,
         duplicate_map, canon_map, image_rows) = load_state()
        st.info(f"Resumed with {len(visited)} URLs.")
    elif not resume and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    rp = fetch_robots(base_url)
    for seed in seed_from_sitemap(base_url) or [base_url]:
        crawl(seed, base_url, 0, rp)

    save_state()
    progress_bar.progress(1.0)
    status_txt.text(f"Finished – {pages_crawled} pages.")

    if not rows:
        st.warning("No pages crawled.")
        st.stop()

    df = pd.DataFrame(rows).drop_duplicates("URL")
    st.dataframe(df, use_container_width=True)

    # (Downloads, email, etc. – unchanged from your previous version)
