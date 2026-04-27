import pandas as pd

spark_df = pd.read_csv("spark_features.csv")
neo4j_df = pd.read_csv("neo4j_graph_features.csv")

final_df = spark_df.merge(neo4j_df, on="user_id", how="left")
final_df = final_df.fillna(0)

final_df = final_df.drop_duplicates(subset=["user_id", "window_start", "window_end"])

final_df.to_csv("final_features_for_ml.csv", index=False)

print("Saved final_features_for_ml.csv")
print("Rows:", len(final_df))
print("Columns:", final_df.columns.tolist())
print(final_df.head())