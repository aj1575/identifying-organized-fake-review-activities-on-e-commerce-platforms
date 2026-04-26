# Identifying Organized Fake Review Activities on E-Commerce Platforms

## 📌 Project Overview

This project aims to detect **organized fake review activities** on e-commerce platforms using a **real-time data pipeline** and **machine learning features**.

The system combines:
- **Apache Kafka** → streaming data ingestion  
- **Apache Spark Structured Streaming** → real-time processing  
- **Neo4j Graph Database** → relationship modeling  
- **Feature Engineering** → for ML model training  

## 📚 Technologies Used
* Python
* Apache Kafka
* Apache Spark
* Neo4j
* Pandas

---

## 🛠️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/identifying-organized-fake-review-activities-on-e-commerce-platforms.git
cd identifying-organized-fake-review-activities-on-e-commerce-platforms
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows (Git Bash)
```

### 3. Install Dependencies

```bash
pip install pandas kafka-python pyspark neo4j
```

### 4. Install Java (Required for Spark)
Install Java 17
Set environment variable:

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
```

### 5. Setup Hadoop (Windows Only)

Download ```winutils.exe``` and place it in:
```text
 ~/hadoop/bin/
 ```

Set environment:

```bash
export HADOOP_HOME="$HOME/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"
```

### 6. Start Kafka

Open new terminal:

```bash
zookeeper-server-start.sh config/zookeeper.properties
```

Open another terminal:

```bash
kafka-server-start.sh config/server.properties
```

### 7. Start Neo4j
* Install Neo4j Desktop or run locally
* Start database

Connection details:

```text
bolt://localhost:7687
username: neo4j
password: your_password
```


## ⚙️ Architecture

```text
CSV Dataset 
    ↓
Kafka Producer 
    ↓
Kafka Topic (reviews_topic)
    ↓
Spark Structured Streaming
    ├── DGIM (burst detection)
    ├── Bloom Filter (suspicious users)
    ├── LSH (duplicate detection)
    ↓
Neo4j Graph Database
    ↓
Feature Extraction
    ↓
Final ML Dataset
```


## 🔄 Pipeline Components

#### 🔹 Kafka Producer
* Reads cleaned dataset
* Injects synthetic spam behavior
* Streams data to Kafka topic

#### 🔹 Spark Streaming Consumer

Processes real-time data using:

##### DGIM
* Detects burst of low ratings
##### Bloom Filter
* Identifies suspicious users
##### LSH
* Detects duplicate reviews
##### Window Aggregation
* Builds user-level behavior

#### 🔹 Neo4j Graph Database

Models relationships:

* ``` User → ReviewWindow → Product```

Used for:

* user overlap
* coordinated attacks
* suspicious patterns

## 🚀 How to Run

### 1. Start Spark Consumer

```bash
python spark_consumer_neo4j.py
```

### 2. Run Kafka Producer

```bash
python producer.py --input amazon_clean.csv --start-row 1 --end-row 7000 --sleep 0.01
```

### 3. Extract Features

```bash
python save_spark_features.py
python extract_neo4j_features.py
python merge_features.py
```

## 📊 Final ML Dataset
``` final_features_for_ml.csv```

#### Dataset Info
* Rows: ~57,000
* Columns: 17
* No missing values

## 📥 Dataset Access

👉 Dataset is shared via Google Drive: final_features_for_ml.csv

[https://drive.google.com/drive/folders/1628IopZ61wqICqIT5bQdb-ADEryiBnoY]

## 🤖 ML Features
#### Spark Features (Behavioral)
* low_rating_reviews_by_user_5m
* distinct_products
* num_products_in_window
* burst_detected
* suspicious_user_flag
#### Neo4j Features (Graph)
* num_windows
* num_suspicious_windows
* num_total_targeted_products
* num_suspicious_products_targeted
* has_multiple_suspicious_windows
* num_other_users_shared_products
* max_common_products_with_any_user

## 🎯 Target Variable
``` suspicious_user_flag```
