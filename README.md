# chatbot_for_higashinaruse

# 東成瀬村 RAGチャットボット システム構成

このリポジトリは、東成瀬村公式サイトの情報（HTML・PDF）を自動収集し、  
住民向けチャットボットで回答を生成するための **RAG (Retrieval-Augmented Generation)** パイプラインです。

---

## 🏗️ システム概要

AWS Lambda を中心とした完全サーバレス構成。  
村公式サイトを定期クロール → テキスト抽出 → ベクトル化 → 検索応答 までを自動連鎖実行します。



---

## 🧩 ディレクトリ構成（S3）

| 第1階層 | 第2階層 | 第3階層 | 概要 |
|:--|:--|:--|:--|
| `cache/` | `2025-xx-xx.jsonl` | | HTML・PDFをテキスト化したチャンクデータ |
| `config/` | `config.json` / `prompt.txt` / `scenario.json` | | チャットUI用設定・システムプロンプト |
| `embeddings/` | `2025-xx-xx_embed_*.jsonl` | | OpenAI APIで生成されたベクトルデータ |
| `reference/` | `vill_reference.json` | | クロール済みURLの一覧（HTML/PDF） |
| `vector/` | `index.jsonl` | | embeddingsを統合した最終検索インデックス |

---

## ⚙️ Lambda関数一覧

| Lambda名 | 実行トリガー | 概要 | 備考 | 主なライブラリ |
|:--|:--|:--|:--|:--|
| **1-build_reference** | 毎週月曜 午前10時 | 村公式サイトをクロールし、HTML/PDFのURLを収集。`reference/vill_reference.json` に保存。 | EventBridge による自動実行 | `requests`, `BeautifulSoup4`, `boto3`, `json` |
| **2-build_cache_dispatcher** | [1-build_reference] の実行後 | `vill_reference.json` をもとにURL群を SQS に投入。`3-build_cache_worker` で分散処理を指示。 | `lambda:InvokeFunction` 権限要 | `boto3`, `json` |
| **3-build_cache_worker** | SQS（URL受信時） | 各URLからHTMLまたはPDFを抽出・分割（チャンク化）。テキストを `cache/` に保存。 | SQSトリガーによる自動実行 | `requests`, `pdfplumber`, `BeautifulSoup4`, `boto3` |
| **4-build_embeddings** | 毎週月曜 午前10時30分 | `cache/` 内のテキストを OpenAI API でベクトル化。結果を `embeddings/` に出力。 | EventBridge による自動実行 | `openai`, `boto3`, `json`, `tqdm` |
| **5-build_vector** | [4-build_embeddings] の実行後 | すべての embeddings を統合し、最終的な `vector/index.jsonl` を生成。 | `lambda:InvokeFunction` 権限要 | `boto3`, `pytz`, `json`, `datetime` |
| **6-chat_query** | API Gateway からリクエスト時 | 住民チャットからの質問を受け、`vector/index.jsonl` を参照してRAG回答を生成。 | API Gateway 経由で呼び出し | `openai`, `boto3`, `json`, `math`, `datetime` |
| **9-test** | 手動実行 | 開発・動作確認用 | テスト用関数 | - |

---

## 🧠 ベクトル検索アルゴリズム

- Embeddingモデル: `text-embedding-3-small`
- 類似度: **コサイン類似度**
- スコア補正ロジック：
  - 年度の新しさ（令和/西暦を自動抽出）
  - ドメイン優先（公式サイト内URLを加点）
  - HTML優先、ただし最新年度PDFは逆転許可
  - post番号による新規性加点
- 類似度スコアが低い場合（0.8未満）は除外、または再スコアリングで補正予定。

---

## 🪶 チャンク化（Cache生成）

`build_cache_worker` にてHTML/PDFを以下のルールで分割。

- 1チャンクあたり 約1000文字
- **タイトル・h1・本文を同一ブロック**に結合（意味単位を保持）
- 改行や箇条書きは維持し、文脈破壊を防止
- PDF → `pdfplumber` / HTML → `BeautifulSoup4`

---

## 💬 チャット回答生成（6-chat_query）

1. ユーザー入力を embedding 化  
2. `vector/index.jsonl` と照合  
3. 上位候補（cosine類似度+スコア補正）を抽出  
4. S3キャッシュから該当テキストをロード  
5. OpenAI `gpt-4o-mini` で回答生成  

### システムプロンプト構造
- Webページを最優先（PDFは補助）
- 古い資料を参照する場合はその旨を明示
- 現在日付を自動付与（例：令和7年10月23日）

---

## 🔐 IAM権限ポリシー

- **lambda:InvokeFunction**  
  → 1→2, 4→5 の連鎖実行用
- **s3:GetObject / PutObject / ListBucket**
- **sqs:SendMessage / ReceiveMessage / DeleteMessage**
- **logs:CreateLogGroup / CreateLogStream / PutLogEvents**
- **secretsmanager:GetSecretValue**（APIキー取得時）

---

## 🕒 自動実行スケジュール（EventBridge）

| 時刻 | Lambda | 内容 |
|:--|:--|:--|
| 毎週月曜 10:00 | `1-build_reference` | サイトクロール・URL更新 |
| 毎週月曜 10:30 | `4-build_embeddings` | ベクトル再生成 |

---

## 🧩 成果物の流れ

| 処理段階 | 出力成果物 | 保存場所 |
|:--|:--|:--|
| クロール | `vill_reference.json` | `reference/` |
| テキスト抽出 | `cache/xxxx.jsonl` | `cache/` |
| ベクトル化 | `embeddings/xxxx.jsonl` | `embeddings/` |
| 統合インデックス | `index.jsonl` | `vector/` |
| 回答生成 | ChatGPT出力 | API応答(JSON) |

---

## 🧰 主要ライブラリ一覧

| 分類 | ライブラリ | 用途 |
|:--|:--|:--|
| AWS | `boto3` | S3, SQS, Lambda 呼び出し |
| HTML処理 | `BeautifulSoup4` | Webページテキスト抽出 |
| PDF処理 | `pdfplumber` | PDFテキスト抽出 |
| Embedding | `openai` | ベクトル生成 (`text-embedding-3-small`) |
| 日時 | `pytz`, `datetime` | JST変換, 実行ログ管理 |
| 汎用 | `json`, `math`, `re` | データ処理, 正規表現, スコア補正 |

---

## 🧾 ログとデバッグ出力（CloudWatch）

出力例：



---

## 🚀 開発・運用Tips

- Embeddingモデル変更時は **全データ再生成が必要**
- `vill_reference.json` は毎週更新（URL構造変動に対応）
- PDFとHTMLで最新年度が異なる場合、年度優先で逆転補正される
- CloudWatchで `score` / `year` / `type` を確認してチューニング可能

---

## 📜 License
このシステム構成・コードは東成瀬村オープンデータ連携開発の一部です。  
再利用・改変は自由ですが、出典を明記してください。

---

### 🧩 作者メモ
構築環境：  
AWS Lambda + S3 + SQS + EventBridge + API Gateway + DynamoDB  
OpenAI API (GPT-4o, Embedding-3-small)
