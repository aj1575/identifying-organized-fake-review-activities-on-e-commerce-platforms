"""
Kafka producer: read standardized cleaned reviews and publish JSON events.

This version injects realistic suspicious behavior without making it too obvious.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from typing import Any

import pandas as pd
from kafka import KafkaProducer


# =========================================================
# 1. REALISTIC SYNTHETIC POOLS
# =========================================================
SYNTHETIC_USER_POOL = [
    "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(28))
    for _ in range(50)
]

SYNTHETIC_PRODUCT_POOL = [
    f"B0{random.randint(10000000, 99999999)}"
    for _ in range(20)
]

SUSPICIOUS_TIME_WINDOWS = [
    "2024-04-01 10",
    "2024-04-01 14",
    "2024-04-02 09",
    "2024-04-02 19",
]

REALISTIC_NEGATIVE_TEMPLATES = [
    "The quality is not as expected. It worked initially but had issues later.",
    "I am not fully satisfied with this product. It could be better.",
    "The product is okay, but I faced some performance problems.",
    "It doesn’t meet the expectations set by the description.",
    "The build quality seems a bit weak for the price.",
    "I had higher expectations, but the performance was average.",
    "It works, but not consistently. Sometimes it behaves unexpectedly.",
    "The product is usable, but there are noticeable issues.",
    "Not the best experience. It could definitely be improved.",
    "The item is average. Not bad, but not great either.",
    "I was disappointed with the performance after a few uses.",
    "The product feels less durable than I expected.",
    "It looked promising at first, but the overall experience was disappointing.",
    "I think the product is overpriced for what it offers.",
    "It performs the basic job, but I expected better quality.",
    "There are some issues that make it hard to recommend.",
    "The design is fine, but the reliability could be improved.",
    "I used it for a short time and started noticing problems.",
    "The item is usable, but not as good as the reviews suggested.",
    "The quality control could definitely be better.",
    "The product arrived fine, but the performance has not been consistent.",
    "This was not the experience I expected from the description.",
    "The overall quality feels average and not very durable.",
    "It works, but I am not completely satisfied with the results.",
    "There are some noticeable drawbacks after regular use.",
    "The product is acceptable, but I expected better performance.",
    "I would say the quality is just average for the price.",
    "The item has some useful aspects, but also several issues.",
    "It is not terrible, but it does not feel reliable enough.",
    "The product does the job, but only at a basic level."
]


# =========================================================
# 2. KAFKA HELPERS
# =========================================================
def serializer(message: dict[str, Any]) -> bytes:
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=serializer,
    )


# =========================================================
# 3. VALIDATION
# =========================================================
def validate_record(record: dict[str, Any]) -> bool:
    required_fields = ["review_id", "user_id", "product_id", "review_text", "timestamp"]
    for field in required_fields:
        val = record.get(field)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return False
        if str(val).strip() == "":
            return False
    return True


# =========================================================
# 4. REALISTIC SPAM INJECTION
# =========================================================
def build_injected_record(row: pd.Series) -> dict[str, Any]:
    base_review_id = str(row.get("review_id", ""))

    user_id = random.choice(SYNTHETIC_USER_POOL)
    product_id = random.choice(SYNTHETIC_PRODUCT_POOL)
    review_text = random.choice(REALISTIC_NEGATIVE_TEMPLATES)

    rating = random.choice([1, 1, 2, 2, 3])

    chosen_hour = random.choice(SUSPICIOUS_TIME_WINDOWS)
    minute = random.randint(0, 44)
    second = random.randint(0, 59)
    millis = random.randint(0, 999)
    timestamp = f"{chosen_hour}:{minute:02d}:{second:02d}.{millis:03d}"

    label = row.get("label")

    return {
        "review_id": base_review_id,
        "user_id": user_id,
        "product_id": product_id,
        "rating": None if pd.isna(rating) else float(rating),
        "review_text": review_text,
        "timestamp": timestamp,
        "label": None if pd.isna(label) else int(label),
        "source": "synthetic_spam",
    }


def build_real_record(row: pd.Series) -> dict[str, Any]:
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
        "source": str(row.get("source", "amazon")),
    }


def row_to_record(row: pd.Series, injection_rate: float) -> dict[str, Any]:
    if random.random() < injection_rate:
        return build_injected_record(row)
    return build_real_record(row)


# =========================================================
# 5. MAIN
# =========================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Stream cleaned reviews to Kafka as JSON.")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="reviews_topic", help="Topic name")
    parser.add_argument("--input", default="amazon_clean.csv", help="Path to cleaned CSV")
    parser.add_argument("--sleep", type=float, default=0.01, help="Seconds between sends")
    parser.add_argument("--max-rows", type=int, default=0, help="If >0, only send first N valid rows in selected range")
    parser.add_argument("--start-row", type=int, default=1, help="1-based row number to start from")
    parser.add_argument("--end-row", type=int, default=0, help="1-based row number to stop at (0 means till end)")
    parser.add_argument("--inject-rate", type=float, default=0.12, help="Fraction of injected suspicious rows")
    args = parser.parse_args()

    if args.start_row < 1:
        raise ValueError("--start-row must be 1 or greater")

    if args.end_row != 0 and args.end_row < args.start_row:
        raise ValueError("--end-row must be 0 or greater than/equal to --start-row")

    if not (0 <= args.inject_rate <= 1):
        raise ValueError("--inject-rate must be between 0 and 1")

    df = pd.read_csv(args.input)
    producer = create_producer(args.broker)

    sent_count = 0
    real_count = 0
    synthetic_count = 0

    for row_num, (_, row) in enumerate(df.iterrows(), start=1):
        if row_num < args.start_row:
            continue

        if args.end_row and row_num > args.end_row:
            break

        record = row_to_record(row, injection_rate=args.inject_rate)

        if not validate_record(record):
            continue

        producer.send(args.topic, value=record)
        sent_count += 1

        if record["source"] == "synthetic_spam":
            synthetic_count += 1
        else:
            real_count += 1

        print(
            f"Sent row {row_num}: {record['review_id']} | "
            f"user={record['user_id']} | product={record['product_id']}"
        )

        if args.sleep > 0:
            time.sleep(args.sleep)

        if args.max_rows and sent_count >= args.max_rows:
            break

    producer.flush()
    producer.close()

    print(f"\nDone. Sent {sent_count} records to Kafka topic '{args.topic}'.")
    print(f"Real rows sent: {real_count}")
    print(f"Injected rows sent: {synthetic_count}")


if __name__ == "__main__":
    main()