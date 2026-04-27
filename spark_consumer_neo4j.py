from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    approx_count_distinct,
    collect_set,
    when,
    udf,
    sha2,
    substring,
    lower,
    regexp_replace,
    trim,
    split,
    size,
    monotonically_increasing_id,
    desc,
    concat_ws,
    max as spark_max
)
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.ml.feature import NGram, HashingTF, MinHashLSH
from neo4j import GraphDatabase
import hashlib

print("RUNNING USER TABLE + DGIM + BLOOM FILTER + LSH + NEO4J")

# =========================================================
# 1. DGIM CLASS
# =========================================================
class DGIM:
    def __init__(self):
        self.buckets = []
        self.current_time = 0

    def add_bit(self, bit):
        self.current_time += 1
        if bit == 1:
            self.buckets.insert(0, (1, self.current_time))
            self._compress_buckets()

    def _compress_buckets(self):
        changed = True
        while changed:
            changed = False
            size_to_indices = {}

            for idx, (bucket_size, end_time) in enumerate(self.buckets):
                size_to_indices.setdefault(bucket_size, []).append(idx)

            for bucket_size, indices in size_to_indices.items():
                if len(indices) > 2:
                    second_oldest_idx = indices[-2]
                    oldest_idx = indices[-1]

                    merged_size = bucket_size * 2
                    merged_end_time = self.buckets[second_oldest_idx][1]

                    del self.buckets[oldest_idx]
                    del self.buckets[second_oldest_idx]

                    self.buckets.append((merged_size, merged_end_time))
                    self.buckets.sort(key=lambda x: x[1], reverse=True)

                    changed = True
                    break

    def count_last_k(self, k):
        threshold_time = self.current_time - k
        total = 0

        for bucket_size, end_time in self.buckets:
            if end_time > threshold_time:
                total += bucket_size
            else:
                total += bucket_size // 2
                break

        return total

    def show_buckets(self):
        return self.buckets


# =========================================================
# 2. BLOOM FILTER CLASS
# =========================================================
class BloomFilter:
    def __init__(self, size=10000, num_hashes=3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [0] * size

    def _hashes(self, item):
        item_str = str(item)
        hash_values = []

        for i in range(self.num_hashes):
            combined = f"{item_str}_{i}".encode("utf-8")
            digest = hashlib.md5(combined).hexdigest()
            hash_index = int(digest, 16) % self.size
            hash_values.append(hash_index)

        return hash_values

    def add(self, item):
        for hash_index in self._hashes(item):
            self.bit_array[hash_index] = 1

    def might_contain(self, item):
        for hash_index in self._hashes(item):
            if self.bit_array[hash_index] == 0:
                return False
        return True


# =========================================================
# 3. GLOBAL OBJECTS
# =========================================================
dgim_low_rating = DGIM()
user_bloom = BloomFilter(size=10000, num_hashes=3)

DGIM_K = 100
DGIM_BURST_THRESHOLD = 30
USER_BURST_THRESHOLD = 3

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"


# =========================================================
# 4. CREATE SPARK SESSION
# =========================================================
spark = (
    SparkSession.builder
    .appName("Fake Review Project + Neo4j")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.local.hostname", "localhost")
    .config("spark.executor.instances", "1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 5. READ STREAM FROM KAFKA
# =========================================================
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "reviews_topic")
    .option("startingOffsets", "latest")
    .load()
)


# =========================================================
# 6. DEFINE SCHEMA
# =========================================================
schema = (
    StructType()
    .add("review_id", StringType())
    .add("user_id", StringType())
    .add("product_id", StringType())
    .add("rating", DoubleType())
    .add("review_text", StringType())
    .add("timestamp", StringType())
    .add("label", StringType())
    .add("source", StringType())
)


# =========================================================
# 7. PARSE JSON
# =========================================================
parsed_df = (
    df.selectExpr("CAST(value AS STRING) AS json_string")
      .select(from_json(col("json_string"), schema).alias("data"))
      .select("data.*")
)


# =========================================================
# 8. CONVERT TIMESTAMP
# =========================================================
timestamp_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS")
)


# =========================================================
# 9. KEEP VALID TIMESTAMPS
# =========================================================
clean_df = timestamp_df.filter(col("event_time").isNotNull())


# =========================================================
# 9.5 K-ANONYMITY
# =========================================================
anonymized_df = clean_df.withColumn(
    "anonymous_user_id",
    substring(sha2(col("user_id"), 256), 1, 12)
)


# =========================================================
# 10. MAIN TABLE LOGIC
# =========================================================
low_rating_df = anonymized_df.filter(col("rating") <= 3)

user_windowed_stats_df = low_rating_df.groupBy(
    window(col("event_time"), "1 hour"),
    col("user_id"),
    col("anonymous_user_id")
).agg(
    count("*").alias("low_rating_reviews_by_user_5m"),
    approx_count_distinct("product_id").alias("distinct_products"),
    collect_set("product_id").alias("product_ids"),
    spark_max(when(col("source") == "synthetic_spam", 1).otherwise(0)).alias("source_spam_label")
)

final_df = user_windowed_stats_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("user_id"),
    col("anonymous_user_id"),
    col("low_rating_reviews_by_user_5m"),
    col("distinct_products"),
    col("product_ids"),
    col("source_spam_label")
)

final_df = final_df.withColumn(
    "num_products_in_window",
    size(col("product_ids"))
)

final_df = final_df.withColumn(
    "burst_detected",
    when(col("low_rating_reviews_by_user_5m") >= USER_BURST_THRESHOLD, 1).otherwise(0)
)

final_df = final_df.withColumn(
    "suspicious_user_flag",
    when(
        (col("burst_detected") == 1) | (col("distinct_products") >= 3),
        1
    ).otherwise(0)
)


# =========================================================
# 11. DGIM FUNCTION
# =========================================================
def process_dgim(batch_df, batch_id):
    global dgim_low_rating

    print("\n" + "=" * 70)
    print(f"BATCH {batch_id}: DGIM LOW-RATING BURST SIGNAL")
    print("=" * 70)

    rows = batch_df.select("event_time", "rating").orderBy("event_time").collect()
    print(f"Number of rows in this batch: {len(rows)}")

    if len(rows) == 0:
        print("No data in this batch for DGIM.")
        return

    one_count = 0
    zero_count = 0

    for row in rows:
        rating = row["rating"]

        if rating is not None and rating <= 2:
            bit = 1
            one_count += 1
        else:
            bit = 0
            zero_count += 1

        dgim_low_rating.add_bit(bit)

    approx_recent_low_ratings = dgim_low_rating.count_last_k(DGIM_K)
    dgim_burst_flag = 1 if approx_recent_low_ratings >= DGIM_BURST_THRESHOLD else 0

    print(f"Bit summary in this batch -> ones: {one_count}, zeros: {zero_count}")
    print(f"Total events processed by DGIM so far: {dgim_low_rating.current_time}")
    print(f"DGIM buckets: {dgim_low_rating.show_buckets()}")
    print(f"Approx low-rating count in last {DGIM_K} events: {approx_recent_low_ratings}")
    print(f"DGIM burst flag: {dgim_burst_flag}")


# =========================================================
# 12. BLOOM FILTER FUNCTION
# =========================================================
def process_bloom_filter(batch_df, batch_id):
    global user_bloom

    print("\n" + "=" * 70)
    print(f"BATCH {batch_id}: BLOOM FILTER FOR SUSPICIOUS USERS")
    print("=" * 70)

    rows_in_batch = batch_df.count()
    print(f"Number of rows in this batch: {rows_in_batch}")

    if rows_in_batch == 0:
        print(f"Batch {batch_id} is empty")
        return

    suspicious_rows = (
        batch_df.filter(col("suspicious_user_flag") == 1)
                .select("user_id")
                .collect()
    )

    for row in suspicious_rows:
        user_id = row["user_id"]
        if user_id is not None:
            user_bloom.add(user_id)

    def check_user(user_id):
        if user_id is None:
            return 0
        return 1 if user_bloom.might_contain(user_id) else 0

    check_user_udf = udf(check_user, IntegerType())

    bloom_result_df = batch_df.withColumn(
        "bloom_seen_before",
        check_user_udf(col("user_id"))
    )

    sample_rows = bloom_result_df.limit(20).collect()

    print("-" * 43)
    print(f"Batch: {batch_id}")
    print("-" * 43)

    for row in sample_rows:
        print(row)

    print(f"Suspicious users added in this batch: {len(suspicious_rows)}")
    print(f"Finished bloom batch: {batch_id}")


# =========================================================
# 13. LSH FUNCTION
# =========================================================
def process_lsh(batch_df, batch_id):
    print("\n" + "=" * 70)
    print(f"BATCH {batch_id}: LSH NEAR-DUPLICATE REVIEW SIGNAL")
    print("=" * 70)

    rows_in_batch = batch_df.count()
    print(f"Number of rows in this batch: {rows_in_batch}")

    if rows_in_batch == 0:
        print("No data in this batch for LSH.")
        return

    lsh_input_df = batch_df.filter(
        col("review_text").isNotNull() & (trim(col("review_text")) != "")
    )

    if lsh_input_df.count() < 2:
        print("Not enough review_text rows for LSH.")
        return

    text_df = lsh_input_df.withColumn(
        "clean_review_text",
        trim(regexp_replace(lower(col("review_text")), r"[^a-z0-9\s]", ""))
    ).filter(col("clean_review_text") != "")

    if text_df.count() < 2:
        print("Not enough cleaned rows for LSH.")
        return

    text_df = text_df.withColumn("row_id", monotonically_increasing_id())

    tokens_df = text_df.withColumn(
        "tokens",
        split(col("clean_review_text"), r"\s+")
    ).filter(size(col("tokens")) >= 3)

    if tokens_df.count() < 2:
        print("Not enough tokenized rows for LSH.")
        return

    ngram = NGram(n=3, inputCol="tokens", outputCol="shingles")
    shingles_df = ngram.transform(tokens_df).filter(size(col("shingles")) > 0)

    if shingles_df.count() < 2:
        print("Not enough shingled rows for LSH.")
        return

    hashing_tf = HashingTF(
        inputCol="shingles",
        outputCol="features",
        numFeatures=10000,
        binary=True
    )
    featured_df = hashing_tf.transform(shingles_df)

    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=3)
    model = mh.fit(featured_df)

    similar_pairs_df = (
        model.approxSimilarityJoin(featured_df, featured_df, 0.3, distCol="jaccard_distance")
             .filter(col("datasetA.row_id") < col("datasetB.row_id"))
             .select(
                 col("datasetA.user_id").alias("user_id_left"),
                 col("datasetA.product_id").alias("product_id_left"),
                 col("datasetB.user_id").alias("user_id_right"),
                 col("datasetB.product_id").alias("product_id_right"),
                 (1 - col("jaccard_distance")).alias("similarity_score")
             )
             .orderBy(desc("similarity_score"))
    )

    match_count = similar_pairs_df.count()
    print(f"Near-duplicate pairs found by LSH: {match_count}")

    if match_count == 0:
        print("No near-duplicate review pairs found in this batch.")
        return

    similar_pairs_df.show(20, truncate=False)


# =========================================================
# 14. NEO4J WRITER FUNCTION
# =========================================================
def write_finaldf_to_neo4j(batch_df, batch_id):
    print("\n" + "=" * 70)
    print(f"BATCH {batch_id}: WRITING FINAL_DF TO NEO4J")
    print("=" * 70)

    rows_in_batch = batch_df.count()
    print(f"Rows going to Neo4j in this batch: {rows_in_batch}")

    if rows_in_batch == 0:
        print("No rows to write to Neo4j.")
        return

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    query = """
    UNWIND $rows AS row

    MERGE (u:User {user_id: row.user_id})
    SET u.anonymous_user_id = row.anonymous_user_id

    MERGE (w:ReviewWindow {
        user_id: row.user_id,
        window_start: row.window_start,
        window_end: row.window_end
    })
    SET w.anonymous_user_id = row.anonymous_user_id,
        w.low_rating_reviews_by_user_5m = row.low_rating_reviews_by_user_5m,
        w.distinct_products = row.distinct_products,
        w.num_products_in_window = row.num_products_in_window,
        w.burst_detected = row.burst_detected,
        w.suspicious_user_flag = row.suspicious_user_flag,
        w.source_spam_label = row.source_spam_label

    MERGE (u)-[:ACTIVE_IN_WINDOW]->(w)

    FOREACH (pid IN row.product_ids |
        MERGE (p:Product {product_id: pid})
        MERGE (w)-[:TARGETED_PRODUCT]->(p)
    )
    """

    selected_rows = batch_df.select(
        "user_id",
        "anonymous_user_id",
        "window_start",
        "window_end",
        "low_rating_reviews_by_user_5m",
        "distinct_products",
        "num_products_in_window",
        "burst_detected",
        "suspicious_user_flag",
        "source_spam_label",
        "product_ids"
    ).toLocalIterator()

    batch_rows = []
    total_count = 0

    with driver.session() as session:
        for row in selected_rows:
            batch_rows.append({
                "user_id": row["user_id"],
                "anonymous_user_id": row["anonymous_user_id"],
                "window_start": str(row["window_start"]),
                "window_end": str(row["window_end"]),
                "low_rating_reviews_by_user_5m": int(row["low_rating_reviews_by_user_5m"]),
                "distinct_products": int(row["distinct_products"]),
                "num_products_in_window": int(row["num_products_in_window"]),
                "burst_detected": int(row["burst_detected"]),
                "suspicious_user_flag": int(row["suspicious_user_flag"]),
                "source_spam_label": int(row["source_spam_label"]),
                "product_ids": row["product_ids"] if row["product_ids"] is not None else []
            })

            if len(batch_rows) == 500:
                session.run(query, rows=batch_rows)
                total_count += len(batch_rows)
                print(f"Inserted {total_count} Neo4j rows in batch {batch_id}...")
                batch_rows = []

        if batch_rows:
            session.run(query, rows=batch_rows)
            total_count += len(batch_rows)

    driver.close()
    print(f"Neo4j write complete for batch {batch_id}. Total rows written: {total_count}")


# =========================================================
# 15. SAVE SPARK FEATURES BATCH FUNCTION
# =========================================================
def save_spark_features_batch(batch_df, batch_id):
    print("\n" + "=" * 70)
    print(f"BATCH {batch_id}: SAVING SPARK FEATURES FOR ML")
    print("=" * 70)

    rows_in_batch = batch_df.count()
    print(f"Rows in Spark feature batch: {rows_in_batch}")

    if rows_in_batch == 0:
        print("No rows to save.")
        return

    output_path = f"spark_features_output/batch_{batch_id}"

    save_df = (
        batch_df.withColumn("product_ids_str", concat_ws(",", col("product_ids")))
                .drop("product_ids")
    )

    save_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    print(f"Saved Spark features to {output_path}")


# =========================================================
# 16. STREAM QUERIES
# =========================================================
query1 = (
    final_df.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .option("numRows", 20)
    .queryName("user_level_table_v2")
    .start()
)

query2 = (
    clean_df.writeStream
    .foreachBatch(process_dgim)
    .outputMode("append")
    .queryName("dgim_output_v2")
    .start()
)

query3 = (
    final_df.writeStream
    .foreachBatch(process_bloom_filter)
    .outputMode("complete")
    .queryName("bloom_filter_output_v2")
    .start()
)

query4 = (
    clean_df.writeStream
    .foreachBatch(process_lsh)
    .outputMode("append")
    .queryName("lsh_output_v2")
    .start()
)

query5 = (
    final_df.writeStream
    .foreachBatch(write_finaldf_to_neo4j)
    .outputMode("complete")
    .queryName("neo4j_output_v2")
    .start()
)

query6 = (
    final_df.writeStream
    .foreachBatch(save_spark_features_batch)
    .outputMode("complete")
    .queryName("save_spark_features_v2")
    .start()
)

spark.streams.awaitAnyTermination()