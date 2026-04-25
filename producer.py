"""
Kafka producer: read standardized cleaned reviews and publish JSON events.

Heavy fraud logic belongs downstream (e.g. Spark); this script only validates lightly and sends.
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any
import random

import pandas as pd
from kafka import KafkaProducer


def serializer(message: dict[str, Any]) -> bytes:
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=serializer,
    )


def validate_record(record: dict[str, Any]) -> bool:
    required_fields = ["review_id", "user_id", "product_id", "review_text", "timestamp"]
    for field in required_fields:
        val = record.get(field)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return False
        if str(val).strip() == "":
            return False
    return True


def row_to_record(row: pd.Series) -> dict[str, Any]:
    rating = row.get("rating")
    label = row.get("label")
    timestamp = str(row.get("timestamp", ""))
    source = str(row.get("source", "unknown"))

    # Stronger synthetic spam injection
    if random.random() < 0.3:
        # Use only a few products so many users hit the same products
        product_id = f"FAKE_PRODUCT_{random.randint(1, 3)}"

        # Use fewer fake users so overlap becomes stronger
        user_id = f"fake_user_{random.randint(1, 5)}"

        # Reuse a few repeated templates for LSH similarity
        review_text = random.choice([
            "This product is terrible. Do not buy.",
            "Worst product ever. Waste of money.",
            "Completely useless. Very bad quality."
        ])

        rating = 1

        # Keep fake reviews inside the same 1-hour window
        timestamp = "2024-04-01 10:15:00.000"

        source = "synthetic_spam"
    else:
        product_id = str(row.get("product_id", ""))
        user_id = str(row.get("user_id", ""))
        review_text = str(row.get("review_text", ""))

    return {
        "review_id": str(row.get("review_id", "")),
        "user_id": user_id,
        "product_id": product_id,
        "rating": None if pd.isna(rating) else float(rating),
        "review_text": review_text,
        "timestamp": timestamp,
        "label": None if pd.isna(label) else int(label),
        "source": source,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream cleaned reviews to Kafka as JSON.")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="reviews_topic", help="Topic name")
    parser.add_argument("--input", default="clean_reviews.csv", help="Path to cleaned CSV")
    parser.add_argument("--sleep", type=float, default=0.2, help="Seconds between sends (streaming sim)")
    parser.add_argument("--max-rows", type=int, default=0, help="If >0, only send first N valid rows in the selected range")
    parser.add_argument(
        "--start-row",
        type=int,
        default=1,
        help="1-based row number to start from (after CSV header)"
    )
    parser.add_argument(
        "--end-row",
        type=int,
        default=0,
        help="1-based row number to stop at (0 means till end)"
    )
    args = parser.parse_args()

    if args.start_row < 1:
        raise ValueError("--start-row must be 1 or greater")

    if args.end_row != 0 and args.end_row < args.start_row:
        raise ValueError("--end-row must be 0 or greater than/equal to --start-row")

    df = pd.read_csv(args.input)
    producer = create_producer(args.broker)

    sent_count = 0

    for row_num, (_, row) in enumerate(df.iterrows(), start=1):
        if row_num < args.start_row:
            continue

        if args.end_row and row_num > args.end_row:
            break

        record = row_to_record(row)
        if not validate_record(record):
            continue

        producer.send(args.topic, value=record)
        sent_count += 1
        print(f"Sent row {row_num}: {record['review_id']} | user={record['user_id']} | product={record['product_id']} | source={record['source']}")

        if args.sleep > 0:
            time.sleep(args.sleep)

        if args.max_rows and sent_count >= args.max_rows:
            break

    producer.flush()
    producer.close()

    print(
        f"Done. Sent {sent_count} records from row range "
        f"{args.start_row} to {args.end_row if args.end_row else 'end'} "
        f"to Kafka topic '{args.topic}'."
    )


if __name__ == "__main__":
    main()