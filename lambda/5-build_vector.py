# build_vector_index.lambda_handler — embeddings統合Lambda

import boto3
import json
import os
import datetime
import pytz

s3 = boto3.client("s3")

BUCKET = os.getenv("BUCKET_NAME", "chat-for-vill-reference")
EMBED_PREFIX = os.getenv("EMBED_PREFIX", "embeddings")
VECTOR_PREFIX = os.getenv("VECTOR_PREFIX", "vector")
MAX_FILES = int(os.getenv("MAX_FILES", "1000"))

def lambda_handler(event, context):
    started = datetime.datetime.now(pytz.timezone("Asia/Tokyo"))
    print(f"🚀 build_vector_index started at {started.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1️⃣ 対象ファイル一覧を取得
    objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=EMBED_PREFIX).get("Contents", [])
    objs = [o for o in objs if o["Key"].endswith(".jsonl")]
    print(f"📦 Found {len(objs)} embedding files")

    if not objs:
        print("⚠️ No embedding files found.")
        return {"statusCode": 404, "body": "No embedding files"}

    # 2️⃣ 各ファイルを読み取って統合
    merged = []
    for obj in objs[:MAX_FILES]:
        key = obj["Key"]
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
        lines = [json.loads(line) for line in body.splitlines() if line.strip()]
        merged.extend(lines)
        print(f"✅ Loaded {len(lines)} vectors from {key}")

    # 3️⃣ 統合データをS3に保存
    out_key = f"{VECTOR_PREFIX}/index.jsonl"
    s3.put_object(
        Bucket=BUCKET,
        Key=out_key,
        Body="\n".join(json.dumps(item, ensure_ascii=False) for item in merged).encode("utf-8"),
        ContentType="application/json"
    )

    elapsed = (datetime.datetime.now(pytz.timezone("Asia/Tokyo")) - started).total_seconds()
    print(f"💾 Saved merged index to s3://{BUCKET}/{out_key}")
    print(f"🧩 Total vectors: {len(merged)}")
    print(f"⏱ Elapsed: {elapsed:.1f}s")

    return {"statusCode": 200, "body": f"OK ({len(merged)} vectors merged)"}
