# ─────────────────────────────────────────────────────────────
# SEO Crawler & Reporter – Streamlit (progress bar + email)
# ─────────────────────────────────────────────────────────────
import os, re, time, pickle, xml.etree.ElementTree as ET, datetime, pathlib, asyncio, ssl
from io import BytesIO
from urllib.parse import urljoin, urlparse

import httpx, requests, smtplib, email
from email.message import EmailMessage
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.parquet as pq
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
inc_pat = st.sidebar.text_input("Include pattern (regex)", "")
exc_pat = st.sidebar.text_input("Exclude pattern (regex)", "")

inc_re = re.compile(inc_pat) if inc_pat else None
exc_re = re.compile(exc_pat) if exc_pat else None

delay_sec = st.sidebar.number_input("Delay between requests (s)", 0.0, 10.0, 0.5, 0.1)
resume   = st.sidebar.checkbox("Resume previous crawl", True)
max_depth= st.sidebar.slider("Max depth", 0, 6, 2)
max_pages= st.sidebar.number_input("Stop after N pages (0 = unlimited)", 0, 100000, 0, 100)


# ───────────────────────── Main UI ─────────────────────────
st.title("SEO Crawler & Reporter 📄")
start_url = st.text_input("Website URL", placeholder="https://example.com")
user_email = st.text_input("Email to receive the report", placeholder="you@example.com")
start_btn = st.button("Start crawl")
progress_bar, status_txt = st.empty(), st.empty()

STATE_FILE = "crawl_state.pkl"
HISTORY_DIR = pathlib.Path("history"); HISTORY_DIR.mkdir(exist_ok=True)

# ───────────────────────── Helper functions ─────────────────────────
def polite_get(u):
    time.sleep(delay_sec); return requests.get(u, timeout=10, headers=HEADERS)

def is_internal(base, link):
    return tldextract.extract(base).registered_domain == tldextract.extract(link).registered_domain

def allowed_path(path):
    return (not inc_re or inc_re.search(path)) and (not exc_re or not exc_re.search(path))

def fetch_robots(root):
    rp = RobotExclusionRulesParser()
    try: rp.parse(polite_get(urljoin(root, "/robots.txt")).text.splitlines())
    except Exception: pass
    return rp

def seed_from_sitemap(root):
    try:
        xml = polite_get(urljoin(root, "/sitemap.xml")).content
        return [loc.text.strip() for loc in ET.fromstring(xml).iter("{*}loc")]
    except Exception: return []

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
        s.starttls(context=ctx); s.login(cfg["user"], cfg["password"]); s.send_message(msg)

# ───────────────────────── Data containers ─────────────────────────
visited, pages_crawled, SAVE_EVERY = set(), 0, 50
rows, broken_links, image_rows = [], [], []
out_links, in_links, duplicate_map, canon_map = {}, {}, {}, {}

def save_state():
    pickle.dump((visited, rows, broken_links, out_links, in_links,
                 duplicate_map, canon_map, image_rows), open(STATE_FILE,"wb"))

def load_state():
    return pickle.load(open(STATE_FILE,"rb")) if os.path.exists(STATE_FILE) else (set(),[],[],{},{},{},{},[])

# ───────────────────────── Core crawler ─────────────────────────
def crawl(url, base, depth, rp):
    global pages_crawled
    # ───── debug lines start ─────
    st.write("🔍 trying:", url)                         # ①
    if url in visited:
        st.write(" ↳ skipped (visited)")               # ②
        return
    if depth > max_depth:
        st.write(" ↳ skipped (depth)")                 # ③
        return
    if not rp.is_allowed(HEADERS["User-Agent"], url):
        st.write(" ↳ blocked by robots.txt")           # ④
        return
    if not allowed_path(urlparse(url).path):
        st.write(" ↳ filtered by regex")               # ⑤
        return
    st.write(" ✔ fetching…")                           # ⑥
    # ───── debug lines end ─────

    visited.add(url)
    if url in visited or depth>max_depth or (max_pages and pages_crawled>=max_pages): return
    if not rp.is_allowed(HEADERS["User-Agent"], url): return
    if not allowed_path(urlparse(url).path): return
    visited.add(url)

    try:
        r = polite_get(url)
        if r.status_code!=200 or "text/html" not in r.headers.get("Content-Type",""): return
        soup = BeautifulSoup(r.text, "html.parser")

        # collect tags
        title = soup.title.string.strip() if soup.title else ""
        desc  = (soup.find("meta", {"name":"description"}) or {}).get("content","").strip()
        htags = " | ".join(h.get_text(strip=True) for h in soup.select("h1,h2,h3,h4,h5,h6")[:20])
        meta_robots = (soup.find("meta", {"name":"robots"}) or {}).get("content","")
        canonical = (soup.find("link", rel="canonical") or {}).get("href","")
        og_title = (soup.find("meta", {"property":"og:title"}) or {}).get("content","")
        og_desc  = (soup.find("meta", {"property":"og:description"}) or {}).get("content","")
        tw_card  = (soup.find("meta", {"name":"twitter:card"}) or {}).get("content","")
        schema_types = " | ".join(sorted({e.get("type","").split("/")[-1]
                                          for e in soup.find_all(attrs={"type":re.compile("schema.org")}) if e.get("type")}))

        rows.append({"URL":url,"Title":title,"Meta description":desc,"H1‑H6":htags,
                     "Meta robots":meta_robots,"Canonical":canonical,"OG title":og_title,
                     "OG description":og_desc,"Twitter card":tw_card,"Schema types":schema_types})
        canon_map[url]=canonical
        duplicate_map.setdefault((title+desc).lower().strip(),[]).append(url)

        links_here=set()
        for a in soup.find_all("a",href=True):
            link=urljoin(url,a["href"].split("#")[0])
            if link.startswith(("mailto:","tel:","javascript:")): continue
            links_here.add(link)
            out_links.setdefault(url,set()).add(link)
            in_links[link]=in_links.get(link,0)+1
        for img in soup.find_all("img",src=True):
            image_rows.append({"Page":url,"Image":urljoin(url,img["src"]),
                               "Alt":img.get("alt",""),"Width":img.get("width",""),
                               "Height":img.get("height","")})

        # progress update
        progress_bar.progress(min(pages_crawled/max_pages,1.0) if max_pages else (pages_crawled%100)/100)
        status_txt.text(f"Crawled {pages_crawled}{' / '+str(max_pages) if max_pages else ''} pages…")
        pages_crawled+=1; 
        if pages_crawled%SAVE_EVERY==0: save_state()

        for link in links_here:
            if urlparse(link).scheme in ("http","https") and is_internal(base,link):
                crawl(link,base,depth+1,rp)
    except Exception as e:
        st.error(f"{url} → {e}")

# ───────────────── Async audits ─────────────────
async def fetch_head(session,u):
    try: r=await session.head(u,follow_redirects=True,timeout=10); return u,r.status_code
    except Exception: return u,None

async def audit_links_and_images():
    hrefs={t for ts in out_links.values() for t in ts}; imgs={r["Image"] for r in image_rows}
    async with httpx.AsyncClient(headers=HEADERS) as s:
        link_res=await asyncio.gather(*[fetch_head(s,u) for u in hrefs])
        img_res=await asyncio.gather(*[fetch_head(s,u) for u in imgs])
    status=dict(link_res); img_status=dict(img_res)
    for src,ts in out_links.items():
        for t in ts:
            c=status.get(t); 
            if c and c>=400: broken_links.append({"Source":src,"Href":t,"Status":c,
                                                  "Type":"internal" if is_internal(start_url,t) else "external"})
    for r in image_rows: r["Status"]=img_status.get(r["Image"])

# ───────────────────────── Run ─────────────────────────
if start_btn and start_url:
    base_url=start_url.strip().rstrip("/")
    if resume and os.path.exists(STATE_FILE):
        (visited,rows,broken_links,out_links,in_links,duplicate_map,canon_map,image_rows)=load_state()
        st.info(f"Resumed with {len(visited)} URLs.")
    elif not resume and os.path.exists(STATE_FILE): os.remove(STATE_FILE)

    rp=fetch_robots(base_url)
    for seed in seed_from_sitemap(base_url) or [base_url]:
        crawl(seed,base_url,0,rp)

    asyncio.run(audit_links_and_images()); save_state()
    progress_bar.progress(1.0); status_txt.text(f"Finished – {pages_crawled} pages.")

    if not rows: st.warning("No pages crawled."); st.stop()

    df=pd.DataFrame(rows).drop_duplicates("URL")
    cols=["URL","Title","Meta description","H1‑H6","Meta robots","Canonical",
          "OG title","OG description","Twitter card","Schema types"]
    df=df[cols]
    df["Title empty"]=df["Title"]==""; df["Title too long"]=df["Title"].str.len()>60
    df["Desc empty"]=df["Meta description"]==""; df["Desc too long"]=df["Meta description"].str.len()>155
    df["Title duplicate"]=df.duplicated("Title",keep=False)
    df["Indexability"]=df.apply(lambda r:"Noindex" if "noindex" in r["Meta robots"].lower()
                                else ("Canonicalized" if r["Canonical"] and r["Canonical"]!=r["URL"] else "Indexable"),axis=1)

    dup_df=pd.DataFrame([{"Cluster":k[:60]+"…","URLs":" | ".join(v)} for k,v in duplicate_map.items() if len(v)>1])
    can_df=pd.DataFrame([{"Page":s,"Target":t,"Issue":"Loop" if canon_map.get(t)==s else "Target not crawled"}
                         for s,t in canon_map.items() if t and t!=s and (canon_map.get(t)==s or t not in in_links)])
    broken_df=pd.DataFrame(broken_links); image_df=pd.DataFrame(image_rows)
    orphan_df=pd.DataFrame([{"URL":u} for u in df["URL"] if in_links.get(u,0)==0])

    tabs=st.tabs(["All","Issues","Broken","Duplicates","Canonicals","Graph","Images","Orphans"])
    tabs[0].dataframe(df,use_container_width=True)
    tabs[1].dataframe(df[(df["Title empty"]|df["Title too long"]|df["Title duplicate"]|
                          df["Desc empty"]|df["Desc too long"]|(df["Indexability"]!="Indexable"))],
                      use_container_width=True)
    tabs[2].dataframe(broken_df,use_container_width=True)
    tabs[3].dataframe(dup_df,use_container_width=True)
    tabs[4].dataframe(can_df,use_container_width=True)
    with tabs[5]:
        dot=Digraph()
        for s,ts in list(out_links.items())[:200]:
            for t in list(ts)[:50]:
                if is_internal(base_url,t): dot.edge(s,t)
        st.graphviz_chart(dot)
    tabs[6].dataframe(image_df,use_container_width=True)
    tabs[7].dataframe(orphan_df,use_container_width=True)

    df["Depth"]=df["URL"].str.count("/")-base_url.count("/")
    fig=plt.figure(); df[df["Meta description"]==""].groupby("Depth").size().plot(kind="bar")
    plt.title("Missing Meta Descriptions by Depth"); plt.xlabel("Depth"); plt.ylabel("Pages")
    st.pyplot(fig)

    st.download_button("CSV",df.to_csv(index=False).encode("utf-8"),"crawl.csv","text/csv")
    st.download_button("JSON",df.to_json(orient="records",indent=2).encode("utf-8"),"crawl.json","application/json")
    xbuf=BytesIO()
    with pd.ExcelWriter(xbuf,engine="xlsxwriter") as w:
        df.to_excel(w,index=False,sheet_name="Pages")
        broken_df.to_excel(w,index=False,sheet_name="Broken")
        dup_df.to_excel(w,index=False,sheet_name="Duplicates")
        can_df.to_excel(w,index=False,sheet_name="Canonicals")
    st.download_button("Excel",xbuf.getvalue(),"crawl.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Send email
    if user_email:
        try:
            files=[("crawl.xlsx",xbuf.getvalue(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
            send_email_smtp(user_email,
                            f"SEO crawl report – {base_url}",
                            f"Hi,\n\nFind attached the SEO crawl report for {base_url}"
                            f" ({pages_crawled} pages).\n\nRegards,\nSEO Crawler Bot",
                            files)
            st.success(f"Report emailed to {user_email}")
        except Exception as e:
            st.error(f"Email send failed: {e}")
    else:
        st.info("Enter an email in the sidebar to receive the report.")

    st.caption(f"Crawl UA: `{HEADERS['User-Agent']}`")
