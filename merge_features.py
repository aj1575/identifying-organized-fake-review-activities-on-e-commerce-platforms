import pandas as pd

spark_df = pd.read_csv("spark_features.csv")
neo4j_df = pd.read_csv("neo4j_graph_features.csv")

final_df = spark_df.merge(neo4j_df, on="user_id", how="left")
final_df = final_df.fillna(0)

final_df.to_csv("final_features_for_ml.csv", index=False)

print("Saved final_features_for_ml.csv")
print(final_df.head())
print("\nColumns:")
print(final_df.columns.tolist())