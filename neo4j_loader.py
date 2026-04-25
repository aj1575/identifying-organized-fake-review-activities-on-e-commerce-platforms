from neo4j import GraphDatabase
import pandas as pd

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "password123"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def create_constraints(tx):
    tx.run("CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE")
    tx.run("CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE")

def insert_review(tx, row):
    tx.run(
        """
        MERGE (u:User {user_id: $user_id})
        MERGE (p:Product {product_id: $product_id})
        MERGE (u)-[r:REVIEWED {review_id: $review_id}]->(p)
        SET r.rating = $rating,
            r.review_text = $review_text,
            r.timestamp = $timestamp,
            r.label = $label,
            r.source = $source
        """,
        user_id=str(row["user_id"]),
        product_id=str(row["product_id"]),
        review_id=str(row["review_id"]),
        rating=int(row["rating"]) if pd.notna(row["rating"]) and str(row["rating"]).strip() != "" else None,
        review_text=str(row["review_text"]),
        timestamp=str(row["timestamp"]),
        label=str(row["label"]) if pd.notna(row["label"]) else "",
        source=str(row["source"]),
    )

def main():
    df = pd.read_csv("amazon_clean.csv")

    with driver.session() as session:
        session.execute_write(create_constraints)

        for _, row in df.iterrows():
            if pd.isna(row["user_id"]) or pd.isna(row["product_id"]) or pd.isna(row["review_id"]):
                continue
            if str(row["user_id"]).strip() == "" or str(row["product_id"]).strip() == "" or str(row["review_id"]).strip() == "":
                continue
            session.execute_write(insert_review, row)

    driver.close()
    print("Data loaded into Neo4j successfully.")

if __name__ == "__main__":
    main()