📊 Real-Time Fake Review Detection System (Kafka + Spark + Streaming Algorithms)
🚀 Overview

This project builds a real-time streaming pipeline to detect suspicious review behavior on e-commerce platforms using:

Apache Kafka → streaming data ingestion
Apache Spark Structured Streaming → real-time processing
Streaming Algorithms:
DGIM → burst detection
Bloom Filter → suspicious user tracking
LSH (MinHash) → near-duplicate review detection
Privacy Layer:
K-Anonymity (via hashing user IDs)
🧠 Problem Statement

Detect:

📉 Burst of low ratings
👤 Suspicious users
📝 Duplicate / spam reviews

…in real-time streaming data

🏗️ Architecture
Producer → Kafka Topic → Spark Streaming → Detection Modules
                                         ↓
            -------------------------------------------------
            | DGIM | Bloom Filter | LSH | K-Anonymity |
            -------------------------------------------------
                                         ↓
                                   Console Output
📂 File Reference

Main implementation:

⚙️ Components Explained
1️⃣ Kafka Producer
Sends review data to topic: reviews_topic

Each message contains:

review_id, user_id, product_id, rating, review_text, timestamp
2️⃣ Spark Streaming Pipeline
Step Flow:
Read from Kafka
Parse JSON
Convert timestamp
Filter valid records
Apply detection modules
🔍 Detection Modules
🔴 1. DGIM (Burst Detection)

Detects sudden spike of low ratings

How:

Converts ratings → bit stream

rating <= 2 → 1
else → 0
Maintains compressed buckets
Estimates last K events
Config:
DGIM_K = 100
DGIM_BURST_THRESHOLD = 30
Output:
Approx low-rating count in last K events
DGIM burst flag
🟡 2. User-Level Burst Detection

Inside Spark aggregation:

USER_BURST_THRESHOLD = 2

If a user gives ≥ 2 low ratings in window → flagged

🟢 3. Bloom Filter (Suspicious Users)

Stores users who triggered burst

Why:
Memory efficient
Fast lookup
Behavior:
If user previously flagged → still suspicious
Output:
suspicious_user_flag = 1 or 0
🔵 4. LSH (Near Duplicate Reviews)

Detects copy-paste / spam reviews

Steps:
Clean text
Tokenize
Create 3-word shingles
Hash → vector
MinHash LSH
Compare pairs
Output:
user_id_left | user_id_right | similarity_score
Interpretation:
similarity = 1 → identical reviews
similarity ≈ 0.8+ → suspicious
🟣 5. K-Anonymity (Privacy Layer)

Adds anonymized column:

anonymous_user_id = sha2(user_id)
Why:
Protect user identity
Still allow analysis
Important:
Does NOT affect detection
Only for privacy
📊 Output Tables
✅ Main User Table
Column	Description
window_start	Window start time
window_end	Window end time
user_id	Original ID
anonymous_user_id	Hashed ID
low_rating_reviews_by_user_5m	Count
distinct_products	Unique products
burst_detected	1/0
suspicious_user_flag	1/0
✅ DGIM Output
Approx low-rating count
DGIM buckets
Burst flag
✅ Bloom Filter Output
Row-level suspicious_user_flag
✅ LSH Output
Column	Meaning
user_id_left/right	Users
product_id_left/right	Products
similarity_score	Review similarity
jaccard_distance	Distance
⚠️ Important Observations
1. Why no burst sometimes?
Window too small
Threshold too high
Data not dense
2. Why similarity = 1?
Reviews are identical after cleaning + shingling
3. Why LSH runs even without burst?
Independent module
Works on all low-rating reviews
4. Why Bloom shows 0?
No user crossed threshold
🧪 How to Run
Step 1: Start Kafka
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
Step 2: Create topic
kafka-topics.sh --create --topic reviews_topic --bootstrap-server localhost:9092
Step 3: Run producer
python producer.py
Step 4: Run Spark consumer
python spark_consumer.py
🎯 Demo Tips (Important)

For presentation:

Use:

USER_BURST_THRESHOLD = 2
window = "1 hour"
Or send dense data quickly
💡 Future Improvements
Integrate all signals into single fraud score
Add ML model (Isolation Forest)
Store results in DB instead of console
Real-time dashboard (Grafana)
🧑‍💻 Tech Stack
Python
Apache Kafka
Apache Spark (Structured Streaming)
PySpark ML (LSH)
Streaming Algorithms (DGIM, Bloom)
📌 Final Summary
Module	Purpose
DGIM	Global burst detection
User Threshold	Per-user burst
Bloom Filter	Track suspicious users
LSH	Detect duplicate reviews
K-Anonymity	Privacy

If you want, next I can give you:

👉 Perfect 2-minute explanation for presentation
👉 Architecture diagram for PPT
👉 Expected output screenshots explanation

Just tell 👍
