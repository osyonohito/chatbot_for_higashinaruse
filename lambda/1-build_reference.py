# build_reference.lambda_handler  — 要件①〜④ 準拠の完成版
import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import pytz
from urllib.parse import urljoin, urlsplit, urlunsplit
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3, os

BASE_URL = os.getenv("BASE_URL", "https://vill.higashinaruse.lg.jp/")
OUT_FILE = "vill_reference.json"
DEPTH = 4
DELAY = 0.3

def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; vill-crawler/1.0)"})
    retry = Retry(total=3, backoff_factor=0.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "HEAD"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def normalize(url: str) -> str:
    """クエリは原則すべて削除・フラグメント削除、末尾スラッシュを統一（PDFは除く）"""
    s = urlsplit(url)
    path = s.path
    if not path.endswith(".pdf"):
        if path and path != "/" and not path.endswith("/"):
            path += "/"
    # クエリは空に、フラグメントも空に
    return urlunsplit((s.scheme, s.netloc, path, "", ""))

# ① sitemap.xmlからURLをすべて取得
def get_all_sitemap_urls(session):
    print("📄 Collecting URLs from sitemap.xml ...")
    urls = set()
    idx_url = BASE_URL + "sitemap.xml"

    res = session.get(idx_url, timeout=15)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")

    # --- サブサイトマップ一覧を取得 ---
    submaps = [loc.text.strip() for loc in soup.find_all("loc") if loc.text]
    print(f"🗺️ Found {len(submaps)} sub-sitemaps")

    for sm in submaps:
        if not sm.startswith(BASE_URL):
            continue
        try:
            r = session.get(sm, timeout=15)
            r.encoding = "utf-8"
            subsoup = BeautifulSoup(r.text, "html.parser")

            # 各サブマップの <loc> を抽出
            for loc in subsoup.find_all("loc"):
                u = loc.text.strip()
                if u.startswith(BASE_URL):
                    urls.add(normalize(u))
        except Exception as e:
            print(f"⚠️ Failed to fetch {sm}: {e}")
            continue

    print(f"✅ Sitemap URLs collected: {len(urls)}")
    return urls


# ② 実際に辿れるURLを取得 (x) ＝（サイトマップURL集合 ∩ 実到達集合）
def crawl_reachable_urls_within_sitemap(session, start_url, allowed_set, max_depth=4, delay=0.3):
    visited, to_visit, reachable = set(), {normalize(start_url)}, set()

    for depth in range(max_depth):
        print(f"🌿 Depth {depth+1}/{max_depth} - {len(to_visit)} URLs to crawl")
        new_urls = set()

        for url in to_visit:
            if url in visited or not url.startswith(BASE_URL):
                continue
            visited.add(url)
            try:
                res = session.get(url, timeout=15, allow_redirects=True)
                ctype = (res.headers.get("Content-Type") or "").lower()
                if "text/html" not in ctype:
                    # HTML以外（直PDFなど）は (x) には入れない（yで拾う）
                    time.sleep(delay)
                    continue

                # ← サイトマップに載っているURLだけ (x) 候補に採用
                nurl = normalize(url)
                if nurl in allowed_set:
                    reachable.add(nurl)

                soup = BeautifulSoup(res.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    abs_url = normalize(urljoin(url, a["href"].strip()))
                    if abs_url.startswith(BASE_URL):
                        new_urls.add(abs_url)

                time.sleep(delay)
            except Exception:
                continue

        to_visit = new_urls - visited

    print(f"✅ Reachable URLs (within sitemap): {len(reachable)}")
    return reachable

# ③ 実到達URL (x) の各ページからPDFリンクを抽出 (y)
def extract_pdf_links_from_pages(session, pages):
    pdfs = set()
    print("📑 Extracting PDF links from (x) pages...")
    total = len(pages)
    for i, url in enumerate(pages, 1):
        try:
            res = session.get(url, timeout=15)
            ctype = (res.headers.get("Content-Type") or "").lower()
            if "text/html" not in ctype:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            for a in soup.find_all("a", href=True):
                abs_url = normalize(urljoin(url, a["href"].strip()))
                if abs_url.startswith(BASE_URL) and abs_url.lower().endswith(".pdf"):
                    pdfs.add(abs_url)
        except Exception:
            pass
        if i % 25 == 0:
            print(f"  ... scanned {i}/{total} pages")
        time.sleep(DELAY)
    print(f"✅ PDF links found: {len(pdfs)}")
    return pdfs

def lambda_handler(event=None, context=None):
    started = datetime.datetime.now(pytz.timezone("Asia/Tokyo"))

    session = make_session()

    # Step①: サイトマップの正規URL全集合
    sitemap_urls = get_all_sitemap_urls(session)

    # Step②: トップを起点にクロールしつつ、(x) = サイトマップ内に限定して実到達URLだけ収集
    x_pages = crawl_reachable_urls_within_sitemap(
        session, BASE_URL, allowed_set=sitemap_urls, max_depth=DEPTH, delay=DELAY
    )

    # Step③: (x) のページから見えるPDFを抽出（PDF自体はサイトマップ外でもOK）
    y_pdfs = extract_pdf_links_from_pages(session, sorted(x_pages))

    # Step④: (x) ∪ (y) を出力
    unified = sorted(x_pages.union(y_pdfs))
    data = {"base_url": BASE_URL, "total": len(unified), "links": unified}

    s3 = boto3.client("s3")
    bucket = os.getenv("BUCKET_NAME", "chat-for-vill-reference")
    key = os.getenv("REFERENCE_PREFIX", "reference/") + "vill_reference.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )

    finished = datetime.datetime.now(pytz.timezone("Asia/Tokyo"))
    elapsed = (finished - started).total_seconds()
    print(f"🌐 Total links (HTML + PDF): {len(unified)}")
    print(f"⏱ Elapsed: {elapsed:.1f}s")
    print(f"✅ Saved to S3: s3://{bucket}/reference/vill_reference.json")

    # ✅ Step⑤: build_cache_dispatcher を呼び出し（非同期）
    try:
        lambda_client = boto3.client("lambda")
        dispatcher_arn = os.getenv("DISPATCHER_LAMBDA_ARN")
        if dispatcher_arn:
            lambda_client.invoke(
                FunctionName=dispatcher_arn,
                InvocationType="Event",  # 非同期実行
                Payload=json.dumps({"trigger": "from_reference"})
            )
            print(f"🚀 Triggered build_cache_dispatcher ({dispatcher_arn})")
        else:
            print("⚠️ DISPATCHER_LAMBDA_ARN not set, skipping dispatcher invoke")
    except Exception as e:
        print(f"⚠️ Failed to trigger dispatcher: {e}")

    # ✅ Lambdaハンドラーの正式な戻り値
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "vill_reference.json generated successfully",
            "total": len(unified),
            "elapsed_sec": elapsed
        }, ensure_ascii=False)
    }