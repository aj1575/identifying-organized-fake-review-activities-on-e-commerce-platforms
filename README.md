# identifying-organized-fake-review-activities-on-e-commerce-platforms.

DATA228-style pipeline: **clean two review sources in Jupyter** into one schema, **export CSV + JSONL**, then **stream rows to Kafka** with a small Python producer. Heavier work (dedup at scale, MinHash/LSH, burst detection, Spark) is intended **downstream** of the topic, not in this repo.

## Pipeline

1. **`data_cleaning.ipynb`** — Load Amazon Electronics-style JSONL (gzip) and the deceptive-opinion spam CSV, normalize text and labels, concatenate, write artifacts.
2. **`producer.py`** — Read `clean_reviews.csv`, lightly validate required fields, publish JSON to Kafka (default topic `reviews_topic`).

## Standard schema

All exported review rows use the same columns:

| Column        | Description |
|---------------|-------------|
| `review_id`   | Stable id (`amz_*` or `spam_*`) |
| `user_id`     | Amazon reviewer id or synthetic id for spam rows |
| `product_id`  | `parent_asin` / `asin` (Amazon) or hotel name (spam) |
| `rating`      | Numeric stars on Amazon; often empty for spam |
| `review_text` | Cleaned, lowercased review body (and title for Amazon) |
| `timestamp`   | Review time when available |
| `label`       | `1` deceptive, `0` truthful; **missing (NaN)** for Amazon |
| `source`      | `amazon` or `deceptive_spam` |

## Repository layout

| Path | Role |
|------|------|
| `data_cleaning.ipynb` | Ingest, clean, merge, save outputs |
| `producer.py` | Kafka producer CLI |
| `requirements.txt` | `pandas`, `numpy`, `kafka-python`, `datasets` (optional notebook cell) |
| `data/deceptive-opinion.csv` | Spam corpus (tracked) |
| `clean_reviews.csv` / `clean_reviews.json` | Combined dataset for streaming or further analysis |
| `amazon_clean.csv` / `spam_clean.csv` | Per-source cleaned splits |

Large raw dumps under `data/*.jsonl.gz` are **gitignored** (GitHub file-size limits). Obtain `Electronics.jsonl.gz` (and optional `meta_Electronics.jsonl.gz`) separately and place them in `data/` before running the notebook end-to-end.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Regenerating cleaned files

1. Open `data_cleaning.ipynb` and set paths in the config cell (`DATA_DIR`, `AMAZON_PATH`, `SPAM_CSV_PATH`).
2. **`AMAZON_NROWS`** — Use an integer (e.g. `50_000`) while iterating; set to **`None`** only if you intend to read the full Amazon gzip (large memory/time cost).
3. Run all cells through the export cell to refresh `amazon_clean.csv`, `spam_clean.csv`, `clean_reviews.csv`, and `clean_reviews.json`.

## Kafka producer

With a broker reachable at `localhost:9092` and a topic created (example name matches the default):

```bash
# Example: create topic (command varies by Kafka distribution)
# kafka-topics.sh --create --topic reviews_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

python producer.py \
  --broker localhost:9092 \
  --topic reviews_topic \
  --input clean_reviews.csv \
  --sleep 0.2 \
  --max-rows 0
```

Use `--max-rows N` to send only the first *N* valid rows for smoke tests. Rows missing required fields (`review_id`, `user_id`, `product_id`, `review_text`, `timestamp`) are skipped.

## Optional: Hugging Face sample

The notebook includes a **commented** cell to pull a small slice of `McAuley-Lab/Amazon-Reviews-2023` via `datasets` into a local JSONL file. Uncomment after `pip install datasets` if you want that path instead of a local gzip.

## Clone URL note

The GitHub repository name ends with a **period**, so the remote URL ends with `..git` (`…platforms.` + `.git`). Use the clone URL shown on the repository’s **Code** button on GitHub to avoid typos.
