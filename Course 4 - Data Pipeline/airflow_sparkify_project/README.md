
# Sparkify Data Pipeline with Apache Airflow

This project builds a data pipeline for Sparkify, a music streaming company. The pipeline uses **Apache Airflow** to automate the ETL process from raw JSON files in **Amazon S3** to an analytics-ready schema in **Amazon Redshift**.

---

## 🔧 Project Features

- **DAG Orchestration** with Apache Airflow
- **Staging** JSON logs from S3 into Redshift
- **Star Schema Modeling** (Fact & Dimension tables)
- **Templated SQL** using Airflow macros for backfill
- **Custom Operators**:
    - `StageToRedshiftOperator`
    - `LoadFactOperator`
    - `LoadDimensionOperator`
    - `DataQualityOperator`
- **Data Quality Checks** using assertive and lambda-style expectations

---

## 📁 Project Structure

```
airflow_sparkify_project/
│
├── dags/
│   └── sparkify_dag.py
│
├── plugins/
│   ├── operators/
│   │   ├── stage_redshift.py
│   │   ├── load_fact.py
│   │   ├── load_dimension.py
│   │   ├── data_quality.py
│   │   └── load_redshift.py
│   └── helpers/
│       └── sql_queries.py
│
├── sql_queries.sql
├── requirements.txt
└── README.md
```

---

## 🧬 Pipeline Overview

1. **Begin Execution** (`DummyOperator`)
2. **Stage Data** from S3 to Redshift:
    - `Stage_events`
    - `Stage_songs`
3. **Load Fact Table**:
    - `Load_songplays_fact_table`
4. **Load Dimension Tables**:
    - `Load_user_dim_table`
    - `Load_song_dim_table`
    - `Load_artist_dim_table`
    - `Load_time_dim_table`
5. **Run Data Quality Checks**
6. **Stop Execution**

---

## ⚙️ DAG Configuration

- Schedule: `@hourly`
- Retries: 3 (delay of 5 minutes)
- Catchup: False
- Dynamic S3 key templating supported (`{{ ds }}`)

---

## 📦 Datasets

| Dataset     | Description                             |
|-------------|-----------------------------------------|
| Log Data    | `s3://udacity-dend/log_data`            |
| Song Data   | `s3://udacity-dend/song-data`           |
| JSON Path   | `s3://udacity-dend/log_json_path.json`  |

> Copy these files to your own bucket in the same region as Redshift for optimal performance.

---

## ✅ Data Quality Checks

Examples included:
- Ensure no null `userId` in `users` table
- Ensure `songplays` has non-zero records
- Custom callables supported for flexible assertions

---

## 🔐 AWS Configuration

Set up connections in Airflow Admin UI:

- **Redshift**: `Conn ID = redshift`, type: Postgres
- **AWS**: `Conn ID = aws_credentials`, type: Amazon Web Services

---

## 🧪 Example SQL from sql_queries.sql

```sql
INSERT INTO songplays (...)
SELECT ...
FROM staging_events e
JOIN staging_songs s ON ...
WHERE e.page = 'NextSong';
```

---

## 📚 How to Run

1. Copy datasets to your own S3 bucket.
2. Set up Airflow connections: `redshift`, `aws_credentials`
3. Start the Airflow webserver and scheduler
4. Enable the DAG: `sparkify_dag`
5. Trigger the DAG manually or wait for schedule

---

## 📦 Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 🏗️ Optional Utilities

- `LoadRedshiftOperator`: Run any custom SQL (e.g., `CREATE SCHEMA`)
- `load_redshift.py` operator available in `plugins/operators/`

---

## Author

This project was developed as part of the Udacity Data Engineering Nanodegree.

