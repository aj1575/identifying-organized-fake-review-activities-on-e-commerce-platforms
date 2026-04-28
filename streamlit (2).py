import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="Fake Review Campaign Detection",
    page_icon="🚨",
    layout="wide"
)

st.markdown("""
<style>
.metric-card {
    background: linear-gradient(135deg, #1f2937, #111827);
    padding: 18px;
    border-radius: 18px;
    border: 1px solid #374151;
}
.big-title {
    font-size: 36px;
    font-weight: 800;
}
.subtitle {
    color: #9ca3af;
    font-size: 17px;
}
.alert-high {
    background-color: #7f1d1d;
    padding: 14px;
    border-radius: 12px;
    color: white;
    margin-bottom: 10px;
}
.alert-medium {
    background-color: #78350f;
    padding: 14px;
    border-radius: 12px;
    color: white;
    margin-bottom: 10px;
}
</style>
""", unsafe_allow_html=True)


# =========================================================
# HELPERS
# =========================================================
@st.cache_data
def load_csv(path):
    if path and os.path.exists(path):
        return pd.read_csv(path)
    return None


def find_file(possible_names):
    for name in possible_names:
        if os.path.exists(name):
            return name
    return None


def risk_level(score):
    if score >= 0.75:
        return "High"
    elif score >= 0.45:
        return "Medium"
    return "Low"


# =========================================================
# LOAD FILES
# =========================================================
final_path = find_file(["final_features_for_ml.csv", "data/final_features_for_ml.csv"])
amazon_path = find_file(["amazon_clean.csv", "data/amazon_clean.csv"])
clean_path = find_file(["clean_reviews.csv", "data/clean_reviews.csv"])
spam_path = find_file(["spam_clean.csv", "data/spam_clean.csv"])
anomaly_path = find_file(["ml_anomaly_results.csv", "data/ml_anomaly_results.csv"])

# Prefer anomaly results if available, because it may already contain anomaly_score/predicted_suspicious
main_path = anomaly_path if anomaly_path else final_path

df = load_csv(main_path)
amazon_df = load_csv(amazon_path)
clean_df = load_csv(clean_path)
spam_df = load_csv(spam_path)

if df is None:
    st.error("No dataset found. Please make sure final_features_for_ml.csv or ml_anomaly_results.csv exists.")
    st.stop()

df = df.copy()

# =========================================================
# CREATE RISK SCORE IF MISSING
# =========================================================
risk_features = [
    "low_rating_reviews_by_user_5m",
    "distinct_products",
    "num_products_in_window",
    "burst_detected",
    "suspicious_user_flag",
    "num_suspicious_windows",
    "num_suspicious_products_targeted",
    "has_multiple_suspicious_windows",
    "num_other_users_shared_products",
    "max_common_products_with_any_user",
]

available_risk_features = [c for c in risk_features if c in df.columns]

if "risk_score" not in df.columns:
    if available_risk_features:
        temp = df[available_risk_features].fillna(0).astype(float)
        normalized = (temp - temp.min()) / (temp.max() - temp.min() + 1e-9)
        df["risk_score"] = normalized.mean(axis=1)
    else:
        df["risk_score"] = 0.0

if "risk_level" not in df.columns:
    df["risk_level"] = df["risk_score"].apply(risk_level)

if "predicted_suspicious" not in df.columns:
    df["predicted_suspicious"] = (df["risk_score"] >= 0.55).astype(int)

# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.title("🚨 Control Panel")

st.sidebar.markdown("### Data Files")
st.sidebar.write(f"Main dataset: `{main_path}`")
st.sidebar.write(f"Final features: `{final_path}`")
st.sidebar.write(f"Amazon clean: `{amazon_path}`")
st.sidebar.write(f"Clean reviews: `{clean_path}`")
st.sidebar.write(f"Spam clean: `{spam_path}`")

risk_threshold = st.sidebar.slider("Risk Threshold", 0.0, 1.0, 0.55, 0.05)

auto_refresh = st.sidebar.checkbox("Auto-refresh simulation", value=False)
if auto_refresh:
    time.sleep(2)
    st.experimental_rerun()

selected_level = st.sidebar.multiselect(
    "Risk Level",
    ["High", "Medium", "Low"],
    default=["High", "Medium", "Low"]
)

filtered_df = df[
    (df["risk_score"] >= risk_threshold) &
    (df["risk_level"].isin(selected_level))
].copy()

# =========================================================
# HEADER
# =========================================================
st.markdown(
    '<div class="big-title">🚨 Organized Fake Review Campaign Detection Dashboard</div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="subtitle">Kafka + Spark Structured Streaming + DGIM + Bloom Filter + LSH + Neo4j + Isolation Forest</div>',
    unsafe_allow_html=True
)

st.divider()

# =========================================================
# KPI CARDS
# =========================================================
total_records = len(df)
flagged_records = len(filtered_df)
high_risk = int((df["risk_level"] == "High").sum())
medium_risk = int((df["risk_level"] == "Medium").sum())
avg_risk = float(df["risk_score"].mean())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Records", f"{total_records:,}")
c2.metric("Flagged Records", f"{flagged_records:,}")
c3.metric("High Risk", f"{high_risk:,}")
c4.metric("Medium Risk", f"{medium_risk:,}")
c5.metric("Avg Risk Score", f"{avg_risk:.2f}")

st.divider()

# =========================================================
# TABS
# =========================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📌 Executive Overview",
    "🔥 Behavioral Signals",
    "🧬 Graph Intelligence",
    "🧪 ML / Anomaly Detection",
    "🚨 Alert Center",
    "📄 Data Explorer"
])

# =========================================================
# TAB 1
# =========================================================
with tab1:
    st.subheader("Campaign Risk Overview")

    col1, col2 = st.columns([1.2, 1])

    with col1:
        fig = px.histogram(
            df,
            x="risk_score",
            nbins=40,
            color="risk_level",
            title="Overall Risk Score Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        level_counts = df["risk_level"].value_counts().reset_index()
        level_counts.columns = ["risk_level", "count"]

        fig = px.pie(
            level_counts,
            names="risk_level",
            values="count",
            hole=0.55,
            title="Risk Level Split"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top Suspicious Records")
    display_cols = ["risk_score", "risk_level"] + available_risk_features
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df.sort_values("risk_score", ascending=False)[display_cols].head(20),
        use_container_width=True
    )

# =========================================================
# TAB 2
# =========================================================
with tab2:
    st.subheader("Spark Streaming Behavioral Features")

    behavioral_cols = [
        "low_rating_reviews_by_user_5m",
        "distinct_products",
        "num_products_in_window",
        "burst_detected",
        "suspicious_user_flag",
        "source_spam_label"
    ]

    available_behavioral = [c for c in behavioral_cols if c in df.columns]

    if available_behavioral:
        selected_feature = st.selectbox("Choose behavioral feature", available_behavioral)

        col1, col2 = st.columns(2)

        with col1:
            fig = px.histogram(
                df,
                x=selected_feature,
                color="risk_level",
                title=f"{selected_feature} Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.box(
                df,
                x="risk_level",
                y=selected_feature,
                title=f"{selected_feature} by Risk Level"
            )
            st.plotly_chart(fig, use_container_width=True)

        if "burst_detected" in df.columns:
            burst_count = int(df["burst_detected"].sum())
            st.warning(f"Detected burst-pattern records: {burst_count:,}")
    else:
        st.info("No behavioral feature columns found.")

# =========================================================
# TAB 3
# =========================================================
with tab3:
    st.subheader("Neo4j Graph-Based Fraud Indicators")

    graph_cols = [
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

    available_graph = [c for c in graph_cols if c in df.columns]

    if available_graph:
        selected_graph_feature = st.selectbox("Choose graph feature", available_graph)

        col1, col2 = st.columns(2)

        with col1:
            fig = px.scatter(
                df,
                x=selected_graph_feature,
                y="risk_score",
                color="risk_level",
                title=f"{selected_graph_feature} vs Risk Score"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            graph_summary = df.groupby("risk_level")[available_graph].mean(numeric_only=True).reset_index()
            fig = px.bar(
                graph_summary,
                x="risk_level",
                y=selected_graph_feature,
                color="risk_level",
                title=f"Average {selected_graph_feature} by Risk Level"
            )
            st.plotly_chart(fig, use_container_width=True)

        cols_to_show = ["risk_score", "risk_level"] + available_graph
        st.dataframe(
            df[cols_to_show].sort_values("risk_score", ascending=False).head(30),
            use_container_width=True
        )
    else:
        st.info("No Neo4j graph features found.")

# =========================================================
# TAB 4
# =========================================================
with tab4:
    st.subheader("ML / Anomaly Detection Analysis")

    if "source_spam_label" in df.columns:
        label_col = "source_spam_label"

        fig = px.histogram(
            df,
            x="risk_score",
            color=label_col,
            nbins=40,
            title="Risk Score Compared With Source Spam Label"
        )
        st.plotly_chart(fig, use_container_width=True)

        predicted = (df["risk_score"] >= risk_threshold).astype(int)
        actual = df[label_col].fillna(0).astype(int)

        tp = int(((predicted == 1) & (actual == 1)).sum())
        tn = int(((predicted == 0) & (actual == 0)).sum())
        fp = int(((predicted == 1) & (actual == 0)).sum())
        fn = int(((predicted == 0) & (actual == 1)).sum())

        cm = pd.DataFrame(
            [[tn, fp], [fn, tp]],
            index=["Actual Normal", "Actual Spam"],
            columns=["Predicted Normal", "Predicted Suspicious"]
        )

        fig = px.imshow(
            cm,
            text_auto=True,
            title="Confusion Matrix Based on Risk Threshold"
        )
        st.plotly_chart(fig, use_container_width=True)

        precision = tp / (tp + fp + 1e-9)
        recall = tp / (tp + fn + 1e-9)
        f1 = 2 * precision * recall / (precision + recall + 1e-9)

        m1, m2, m3 = st.columns(3)
        m1.metric("Precision", f"{precision:.3f}")
        m2.metric("Recall", f"{recall:.3f}")
        m3.metric("F1 Score", f"{f1:.3f}")
    else:
        st.info("source_spam_label not found. Add it from ml_model.py output for validation.")

    st.subheader("Correlation Heatmap")
    numeric_df = df.select_dtypes(include=["int64", "float64"])

    if numeric_df.shape[1] > 1:
        corr = numeric_df.corr(numeric_only=True)
        fig = px.imshow(
            corr,
            title="Feature Correlation Heatmap"
        )
        st.plotly_chart(fig, use_container_width=True)

# =========================================================
# TAB 5
# =========================================================
with tab5:
    st.subheader("Real-Time Style Alert Center")

    alert_df = df.sort_values("risk_score", ascending=False).head(15)

    for _, row in alert_df.iterrows():
        score = row["risk_score"]
        level = row["risk_level"]

        if level == "High":
            st.markdown(
                f"""
                <div class="alert-high">
                🚨 <b>HIGH RISK CAMPAIGN ALERT</b><br>
                Risk Score: {score:.3f}<br>
                Reason: Burst behavior / suspicious graph overlap / repeated activity detected.
                </div>
                """,
                unsafe_allow_html=True
            )
        elif level == "Medium":
            st.markdown(
                f"""
                <div class="alert-medium">
                ⚠️ <b>MEDIUM RISK WARNING</b><br>
                Risk Score: {score:.3f}<br>
                Reason: Some behavioral or graph indicators show abnormal activity.
                </div>
                """,
                unsafe_allow_html=True
            )

        with st.expander("View record details"):
            st.write(row)

# =========================================================
# TAB 6
# =========================================================
with tab6:
    st.subheader("Interactive Data Explorer")

    all_cols = df.columns.tolist()
    default_cols = [c for c in ["user_id", "window_start", "window_end", "risk_score", "risk_level"] if c in all_cols]

    selected_cols = st.multiselect(
        "Select columns to display",
        all_cols,
        default=default_cols if default_cols else all_cols[:min(10, len(all_cols))]
    )

    if not selected_cols:
        st.warning("Please select at least one column.")
    else:
        sort_df = df.copy()

        if "risk_score" in sort_df.columns:
            sort_df = sort_df.sort_values("risk_score", ascending=False)

        st.dataframe(
            sort_df[selected_cols],
            use_container_width=True
        )

    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download Dashboard Dataset",
        csv,
        "fake_review_dashboard_export.csv",
        "text/csv"
    )