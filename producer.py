"""
Kafka producer: read standardized cleaned reviews and publish JSON events.

Heavy fraud logic belongs downstream (e.g. Spark); this script only validates lightly and sends.
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any

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

    return {
        "review_id": str(row.get("review_id", "")),
        "user_id": str(row.get("user_id", "")),
        "product_id": str(row.get("product_id", "")),
        "rating": None if pd.isna(rating) else float(rating),
        "review_text": str(row.get("review_text", "")),
        "timestamp": str(row.get("timestamp", "")),
        "label": None if pd.isna(label) else int(label),
        "source": str(row.get("source", "unknown")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream cleaned reviews to Kafka as JSON.")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="reviews_topic", help="Topic name")
    parser.add_argument("--input", default="clean_reviews.csv", help="Path to cleaned CSV")
    parser.add_argument("--sleep", type=float, default=0.2, help="Seconds between sends (streaming sim)")
    parser.add_argument("--max-rows", type=int, default=0, help="If >0, only send first N valid rows")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    producer = create_producer(args.broker)

    sent_count = 0
    for _, row in df.iterrows():
        record = row_to_record(row)
        if not validate_record(record):
            continue

        producer.send(args.topic, value=record)
        sent_count += 1
        print(f"Sent: {record['review_id']}")

        if args.sleep > 0:
            time.sleep(args.sleep)

        if args.max_rows and sent_count >= args.max_rows:
            break

    producer.flush()
    producer.close()

    print(f"Done. Sent {sent_count} records to Kafka topic '{args.topic}'.")


if __name__ == "__main__":
    main()
