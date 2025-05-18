import streamlit as st
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import tldextract
import pandas as pd

# ---------- Helpers ----------
def is_internal(base_url: str, link: str) -> bool:
    base_domain = tldextract.extract(base_url).registered_domain
    link_domain = tldextract.extract(link).registered_domain
    return (base_domain == link_domain) or (link_domain == '')

def crawl(url: str, max_depth: int, visited: set, rows: list, depth: int = 0):
    if url in visited or depth > max_depth:
        return
    visited.add(url)

    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type", ""):
            return

        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.title.string.strip() if soup.title else ""
        descr_tag = soup.find("meta", attrs={"name": "description"})
        description = descr_tag["content"].strip() if descr_tag and descr_tag.get("content") else ""

        rows.append({"URL": url, "Title (tag)": title, "Meta Description": description})

        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if urlparse(link).scheme in ("http", "https") and is_internal(url, link):
                crawl(link, max_depth, visited, rows, depth + 1)
    except Exception as e:
        st.error(f"Error crawling {url}: {e}")

# ---------- Streamlit UI ----------
st.title("Meta Title & Description Crawler")

start_url = st.text_input("Website URL", placeholder="https://example.com")
max_depth = st.slider("Crawl depth", 0, 5, 2)
run = st.button("Start crawl")

if run and start_url:
    st.info("⏳ Crawling… this may take a moment.")
    visited, rows = set(), []
    crawl(start_url.strip(), max_depth, visited, rows)
    if rows:
        df = pd.DataFrame(rows)
        st.success(f"✅ Finished! Crawled {len(rows)} pages.")
        st.dataframe(df, use_container_width=True)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, file_name="meta_data.csv", mime="text/csv")
    else:
        st.warning("No pages found or no meta data extracted.")
