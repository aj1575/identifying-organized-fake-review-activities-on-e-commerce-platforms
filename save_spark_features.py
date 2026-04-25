import pandas as pd
import glob

# Read all batch csv files inside spark_features_output/batch_*/ folders
files = glob.glob("spark_features_output/batch_*/*.csv")
if not files:
    raise FileNotFoundError("No CSV files found in spark_features_output/batch_*/")

spark_df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

# Keep only useful Spark columns
spark_feature_df = spark_df[
    [
        "user_id",
        "window_start",
        "window_end",
        "low_rating_reviews_by_user_5m",
        "distinct_products",
        "num_products_in_window",
        "burst_detected",
        "suspicious_user_flag"
    ]
].copy()

spark_feature_df.to_csv("spark_features.csv", index=False)
print("Saved spark_features.csv")
print(spark_feature_df.head())