from neo4j import GraphDatabase
import pandas as pd

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "password123"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def run_query(query: str) -> pd.DataFrame:
    with driver.session() as session:
        result = session.run(query)
        rows = [record.data() for record in result]
    return pd.DataFrame(rows)


# 1. Only users that actually appear in the Spark window graph
q1 = """
MATCH (u:User)-[:ACTIVE_IN_WINDOW]->(w:ReviewWindow)
RETURN
    u.user_id AS user_id,
    count(DISTINCT w) AS num_windows,
    count(DISTINCT CASE WHEN w.suspicious_user_flag = 1 THEN w END) AS num_suspicious_windows
"""

# 2. Total targeted products per active user
q2 = """
MATCH (u:User)-[:ACTIVE_IN_WINDOW]->(w:ReviewWindow)-[:TARGETED_PRODUCT]->(p:Product)
RETURN
    u.user_id AS user_id,
    count(DISTINCT p) AS num_total_targeted_products
"""

# 3. Suspicious targeted products only
q3 = """
MATCH (u:User)-[:ACTIVE_IN_WINDOW]->(w:ReviewWindow)-[:TARGETED_PRODUCT]->(p:Product)
WHERE w.suspicious_user_flag = 1
RETURN
    u.user_id AS user_id,
    count(DISTINCT p) AS num_suspicious_products_targeted
"""

# 4. Max / avg products per window
q4 = """
MATCH (u:User)-[:ACTIVE_IN_WINDOW]->(w:ReviewWindow)
OPTIONAL MATCH (w)-[:TARGETED_PRODUCT]->(p:Product)
WITH u.user_id AS user_id, w, count(DISTINCT p) AS products_in_window
RETURN
    user_id,
    max(products_in_window) AS max_products_in_one_window,
    avg(products_in_window) AS avg_products_per_window
"""

# 5. Repeated suspicious behavior
q5 = """
MATCH (u:User)-[:ACTIVE_IN_WINDOW]->(w:ReviewWindow)
WITH u.user_id AS user_id,
     count(DISTINCT CASE WHEN w.suspicious_user_flag = 1 THEN w END) AS suspicious_windows
RETURN
    user_id,
    CASE WHEN suspicious_windows > 1 THEN 1 ELSE 0 END AS has_multiple_suspicious_windows
"""

# 6. Shared-product overlap with other active users
q6 = """
MATCH (u1:User)-[:ACTIVE_IN_WINDOW]->(:ReviewWindow)-[:TARGETED_PRODUCT]->(p:Product)
MATCH (u2:User)-[:ACTIVE_IN_WINDOW]->(:ReviewWindow)-[:TARGETED_PRODUCT]->(p:Product)
WHERE u1.user_id <> u2.user_id
RETURN
    u1.user_id AS user_id,
    count(DISTINCT u2.user_id) AS num_other_users_shared_products
"""

# 7. Pair overlap counts
q7 = """
MATCH (u1:User)-[:ACTIVE_IN_WINDOW]->(:ReviewWindow)-[:TARGETED_PRODUCT]->(p:Product)
MATCH (u2:User)-[:ACTIVE_IN_WINDOW]->(:ReviewWindow)-[:TARGETED_PRODUCT]->(p:Product)
WHERE u1.user_id < u2.user_id
WITH u1.user_id AS user1, u2.user_id AS user2, count(DISTINCT p) AS common_products
RETURN user1, user2, common_products
"""

df1 = run_query(q1)
df2 = run_query(q2)
df3 = run_query(q3)
df4 = run_query(q4)
df5 = run_query(q5)
df6 = run_query(q6)
pair_df = run_query(q7)

# derive max_common_products_with_any_user
if not pair_df.empty:
    left_df = pair_df.groupby("user1", as_index=False)["common_products"].max()
    left_df.columns = ["user_id", "max_common_products_with_any_user"]

    right_df = pair_df.groupby("user2", as_index=False)["common_products"].max()
    right_df.columns = ["user_id", "max_common_products_with_any_user"]

    df7 = pd.concat([left_df, right_df], ignore_index=True)
    df7 = df7.groupby("user_id", as_index=False)["max_common_products_with_any_user"].max()
else:
    df7 = pd.DataFrame(columns=["user_id", "max_common_products_with_any_user"])

feature_df = (
    df1.merge(df2, on="user_id", how="left")
      .merge(df3, on="user_id", how="left")
      .merge(df4, on="user_id", how="left")
      .merge(df5, on="user_id", how="left")
      .merge(df6, on="user_id", how="left")
      .merge(df7, on="user_id", how="left")
)

feature_df = feature_df.fillna(0)

numeric_cols = [
    "num_windows",
    "num_suspicious_windows",
    "num_total_targeted_products",
    "num_suspicious_products_targeted",
    "max_products_in_one_window",
    "avg_products_per_window",
    "has_multiple_suspicious_windows",
    "num_other_users_shared_products",
    "max_common_products_with_any_user"
]
for c in numeric_cols:
    if c in feature_df.columns:
        feature_df[c] = pd.to_numeric(feature_df[c], errors="coerce").fillna(0)

feature_df.to_csv("neo4j_graph_features.csv", index=False)
print("Saved neo4j_graph_features.csv")
print("neo4j rows =", len(feature_df))
print(feature_df.head())

driver.close()