import os
import re
import json
import math
import boto3
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional
from openai import OpenAI

# ====== 設定 ======
S3_BUCKET = os.getenv("S3_BUCKET", "chat-for-vill-reference")
VECTOR_KEY = os.getenv("VECTOR_KEY", "vector/index.jsonl")
CACHE_PREFIX = os.getenv("CACHE_PREFIX", "cache/")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

oa = OpenAI(api_key=OPENAI_API_KEY)
s3 = boto3.client("s3")

_VECTOR_INDEX = None
_CACHE_MAP = None


# ====== Cosine類似度 ======
def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


# ====== ベクトル・キャッシュ読込 ======
def _load_vector_index():
    global _VECTOR_INDEX
    if _VECTOR_INDEX is not None:
        return _VECTOR_INDEX

    prefix = os.getenv("VECTOR_PREFIX", "vector/")
    print(f"[DEBUG] loading all vector files from s3://{S3_BUCKET}/{prefix}")

    _VECTOR_INDEX = []
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in resp:
        print(f"[WARN] no vector objects found under {prefix}")
        return _VECTOR_INDEX

    for obj in resp["Contents"]:
        key = obj["Key"]
        if not key.endswith(".jsonl"):
            continue
        try:
            body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
            for line in body.splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                _VECTOR_INDEX.append(rec)
            print(f"[LOAD] vector file loaded: {key}")
        except Exception as e:
            print(f"[WARN] failed to load {key}: {e}")

    print(f"[LOAD] total vector_index entries={len(_VECTOR_INDEX)}")
    return _VECTOR_INDEX


def _load_cache_map():
    global _CACHE_MAP
    if _CACHE_MAP is not None:
        return _CACHE_MAP

    print(f"[DEBUG] loading all cache files from s3://{S3_BUCKET}/{CACHE_PREFIX}")
    _CACHE_MAP = {}

    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=CACHE_PREFIX)
    if "Contents" not in resp:
        print(f"[WARN] no cache objects found under {CACHE_PREFIX}")
        return _CACHE_MAP

    for obj in resp["Contents"]:
        key = obj["Key"]
        if not key.endswith(".jsonl"):
            continue

        try:
            body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
            for line in body.splitlines():
                if not line.strip():
                    continue
                r = json.loads(line)
                url = r.get("url")
                ci = int(r.get("chunk_index", 0))
                content = r.get("content", "")
                if url and content:
                    _CACHE_MAP[(url, ci)] = content
            print(f"[LOAD] cache file loaded: {key}")
        except Exception as e:
            print(f"[WARN] failed to load {key}: {e}")

    print(f"[LOAD] total cache_map entries={len(_CACHE_MAP)}")
    return _CACHE_MAP

def _detect_year_from_query(query: str):
    """質問文から対象年（西暦）を推定"""
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    current_year = now.year

    if m := re.search(r"令和\s*(\d+)", query):
        return 2018 + int(m.group(1))
    if m := re.search(r"20\d{2}", query):
        return int(m.group(0))
    if "今年" in query:
        return current_year
    if "昨年" in query or "去年" in query:
        return current_year - 1
    if "来年" in query:
        return current_year + 1
    return current_year


def _detect_year_from_text(text: str):
    """本文・URL・ファイル名から年度を推定（令和 or 西暦）
       URL末尾やファイル名（例: /2025/09/file.pdf）も確実に対象に含める"""
    if not text:
        return None

    # 令和 → 西暦換算
    if m := re.search(r"令和\s*(\d+)", text):
        return 2018 + int(m.group(1))

    # 西暦 → URLやファイル名にも対応（/2025/, _2024, 2023.pdfなど）
    if m := re.search(r"(20\d{2})", text):
        year = int(m.group(1))
        now_year = datetime.now(timezone(timedelta(hours=9))).year
        if 2000 <= year <= now_year + 1:
            return year

    return None



def _search_from_vector(query, top_k=20):
    """ベクトル類似検索：最新情報を優先しつつ、HTMLとPDFをバランスよく扱う"""
    emb_q = oa.embeddings.create(
        model="text-embedding-3-small",
        input=query
    ).data[0].embedding

    index = _load_vector_index()
    query_year = _detect_year_from_query(query)

    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    current_year = now.year

    print(f"[SEARCH] query='{query}' (target_year={query_year})")

    # === 1️⃣ 全スコアをまず算出して表示 ===
    raw_scored = []
    for r in index:
        url = r.get("url", "")
        score = _cosine(emb_q, r["embedding"])
        raw_scored.append((score, url))
    raw_scored.sort(key=lambda x: x[0], reverse=True)

    print("───[ RAW COSINE SCORES (TOP 20) ]───")
    for i, (s, u) in enumerate(raw_scored[:20], start=1):
        print(f"  {i:02d}. {u[:80]}  score={s:.3f}")
    print("───────────────────────────────────")

    # === 2️⃣ 類似度0.8未満をカット ===
    scored = []
    for r in index:
        url = r.get("url", "")
        file_name = os.path.basename(url)
        preview = (r.get("preview") or "") + " " + url + " " + file_name
        score = _cosine(emb_q, r["embedding"])
        if score < 0.80:
            continue

        # 年を本文・URL・ファイル名から推定
        page_year = _detect_year_from_text(preview)
        if page_year is None:
            page_year = current_year

        # 📆 年度補正
        diff = page_year - current_year
        if diff == 0:
            score += 0.20
        elif diff == -1:
            score += 0.05
        elif diff <= -2:
            score -= 0.10

        # 🆕 post番号による微加点
        if m := re.search(r"post-(\d+)", url):
            score += int(m.group(1)) / 2_000_000.0

        # 📰 HTML / 📄 PDF 優先順位
        if url.lower().endswith(".pdf"):
            score -= 0.05
            if page_year == current_year:
                score += 0.20
            elif page_year > current_year:
                score += 0.30
        else:
            score += 0.10

        # 🔍 キーワード一致補正
        for kw in re.findall(r"[一-龠ぁ-んァ-ンa-zA-Z0-9]+", query):
            if kw in preview:
                score += 0.03

        scored.append((score, r))

    # === 3️⃣ 並べ替えて上位Nを返す ===
    scored.sort(key=lambda x: x[0], reverse=True)
    hits = [r for _, r in scored[:top_k]]

    print(f"[SEARCH] top={len(hits)} results (最新優先＋0.8cut＋再スコア)")
    for i, (s, r) in enumerate(scored[:5], start=1):
        y = _detect_year_from_text(r.get("url", "") or "")
        print(f"  {i}. {r.get('url')}  year={y}  score={s:.3f}")

    return hits


# ====== 現在日時をプロンプトに反映 ======
def _with_current_date(base_prompt: str) -> str:
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    reiwa = now.year - 2018
    date_str = f"{now.year}年{now.month}月{now.day}日（令和{reiwa}年）"
    return (
        base_prompt.strip()
        + f"\n\n# 現在日時: {date_str}\n"
        "「今年」「来年」「昨年」などの表現は、必ずこの日付を基準に判断すること。\n"
        "古い資料を参照する場合は、その旨を明示して補足すること。"
    )


# ====== 回答生成 ======
def generate_reply(user_message, config, prompt):
    hits = _search_from_vector(user_message)
    cache = _load_cache_map()

    ctx_blocks, sources = [], []
    for h in hits:
        url = h["url"]
        ci = int(h["chunk_index"])
        content = cache.get((url, ci), "")
        if content:
            ctx_blocks.append(f"URL: {url}\n{content[:1000]}")
            sources.append({"url": url, "chunk_index": ci})
    ctx = "\n\n".join(ctx_blocks) if ctx_blocks else "(関連本文なし)"

    system_prompt = _with_current_date(
        prompt or "あなたは東成瀬村の情報に基づいて答えるアシスタントです。"
    ) + "\nWebページを最優先に使い、PDFは最終手段としてのみ利用すること。"

    user_prompt = f"質問: {user_message}\n\n以下は東成瀬村の公式サイト等からの資料じゃ。これ以外を根拠にしてはならぬ。\n\n{ctx}"

    resp = oa.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        max_tokens=800,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    )
    reply = resp.choices[0].message.content.strip()
    print(f"[GEN] reply_len={len(reply)} sources={len(sources)}")
    return reply, sources


# ====== Lambda handler ======
def lambda_handler(event, context):
    try:
        payload = json.loads(event.get("body") or "{}")
        message = payload.get("message", "").strip()
        if not message:
            return {"statusCode": 400, "body": json.dumps({"error": "message required"})}

        config = payload.get("config", {})
        prompt = payload.get("prompt", "")
        options = [{"label": "トップに戻る", "next": "restart"}]

        reply, sources = generate_reply(message, config, prompt)
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "reply": reply, 
                    "sources": sources,
                    "options": options
                    }, 
                    ensure_ascii=False)
        }

    except Exception as e:
        print("❌ ERROR:", repr(e))
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "internal_error", "detail": str(e)}, ensure_ascii=False)
        }
