# build_cache_dispatcher.lambda_handler
import boto3
import json
import os
import datetime
import pytz

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

BUCKET_NAME = os.getenv("BUCKET_NAME", "chat-for-vill-reference")
REFERENCE_KEY = os.getenv("REFERENCE_KEY", "reference/vill_reference.json")
QUEUE_URL = os.getenv("QUEUE_URL")  # 例: https://sqs.ap-northeast-1.amazonaws.com/xxxx/vill-cache-tasks
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

def lambda_handler(event, context):
    started = datetime.datetime.now(pytz.timezone("Asia/Tokyo"))
    print(f"🚀 dispatcher started at {started.strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 1️⃣ URLリストをS3から取得 ---
    print(f"📥 Fetching {REFERENCE_KEY} from s3://{BUCKET_NAME}/")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=REFERENCE_KEY)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    urls = data.get("links", [])
    print(f"🔗 Total URLs: {len(urls)}")

    # --- 2️⃣ URLを分割してSQSに投入 ---
    total_batches = 0
    for i in range(0, len(urls), BATCH_SIZE):
        batch = urls[i:i + BATCH_SIZE]
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({"urls": batch})
        )
        total_batches += 1

    elapsed = (datetime.datetime.now(pytz.timezone("Asia/Tokyo")) - started).total_seconds()
    print(f"✅ Dispatched {total_batches} batches to SQS ({len(urls)} URLs)")
    print(f"⏱ Elapsed: {elapsed:.1f}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Dispatch completed",
            "total_batches": total_batches,
            "total_urls": len(urls),
            "elapsed_sec": elapsed
        }, ensure_ascii=False)
    }
