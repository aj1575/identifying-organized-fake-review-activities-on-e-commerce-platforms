import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix

# =====================================
# 1. LOAD DATA
# =====================================
df = pd.read_csv("final_features_for_ml.csv")

print("Dataset shape:", df.shape)
print("\nColumns:")
print(df.columns.tolist())

if "source_spam_label" not in df.columns:
    raise ValueError("source_spam_label column not found in final_features_for_ml.csv")

print("\nTrue label distribution (only for evaluation):")
print(df["source_spam_label"].value_counts())

# =====================================
# 2. SELECT FEATURES
# =====================================
feature_cols = [
    "low_rating_reviews_by_user_5m",
    "distinct_products",
    "num_products_in_window",
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

missing_cols = [c for c in feature_cols if c not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required feature columns: {missing_cols}")

X = df[feature_cols].copy()
y_true = df["source_spam_label"].copy()

print("\nFeatures used:")
print(feature_cols)

# =====================================
# 3. TRAIN ANOMALY MODEL
# =====================================
contamination = max(0.05, min(0.20, df["source_spam_label"].mean()))

model = IsolationForest(
    n_estimators=100,
    contamination=contamination,
    random_state=42
)

model.fit(X)

pred_raw = model.predict(X)
y_pred = pd.Series([1 if v == -1 else 0 for v in pred_raw], index=df.index)

df["anomaly_score"] = model.decision_function(X)
df["predicted_suspicious"] = y_pred

# =====================================
# 4. EVALUATE
# =====================================
print("\nPredicted suspicious distribution:")
print(df["predicted_suspicious"].value_counts())

print("\nClassification Report (validation against source_spam_label):")
print(classification_report(y_true, y_pred))

print("\nConfusion Matrix:")
print(confusion_matrix(y_true, y_pred))

# =====================================
# 5. TOP ANOMALIES
# =====================================
top_anomalies = df.sort_values("anomaly_score").head(20)

print("\nTop 20 most anomalous rows:")
print(
    top_anomalies[
        [
            "user_id",
            "window_start",
            "window_end",
            "source_spam_label",
            "predicted_suspicious",
            "anomaly_score"
        ] + feature_cols
    ]
)

# =====================================
# 6. SAVE
# =====================================
df.to_csv("ml_anomaly_results.csv", index=False)
print("\nSaved anomaly results to ml_anomaly_results.csv")