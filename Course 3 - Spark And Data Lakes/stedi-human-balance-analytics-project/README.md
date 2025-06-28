# STEDI Human Balance Analytics – Data Lakehouse Project

## 📘 Project Overview

This project simulates a real-world data engineering pipeline using AWS Glue, Spark, S3, and Athena. You are a data engineer for the **STEDI** team, curating motion sensor data collected from **Step Trainer IoT devices** and **mobile accelerometer apps**, preparing it for machine learning.

The goal is to build a privacy-aware **data lakehouse architecture** with trusted and curated zones using AWS services, enabling the data science team to train models to detect human balance movements in real time.

## 🛠 Tools & Services Used

- **AWS Glue Studio & Jobs (PySpark)**
- **AWS S3 (Landing, Trusted, Curated Zones)**
- **AWS Athena (DDL + Data Inspection)**
- **AWS Glue Catalog**
- **GitHub (for code submission)**

## 🗂 Directory Structure

```
stedi-human-balance-analytics/
├── ddl/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   └── step_trainer_landing.sql
├── scripts/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── step_trainer_trusted.py
│   └── machine_learning_curated.py
├── screenshots/
│   ├── customer_landing.png
│   ├── accelerometer_landing.png
│   ├── step_trainer_landing.png
│   ├── customer_trusted.png
│   ├── accelerometer_trusted.png
│   ├── step_trainer_trusted.png
│   ├── customer_curated.png
│   └── machine_learning_curated.png
└── README.md
```

## 📁 S3 Folder Setup

Create these folders in your AWS S3 bucket:

```
s3://<your-bucket>/landing/customer_landing/
s3://<your-bucket>/landing/accelerometer_landing/
s3://<your-bucket>/landing/step_trainer_landing/
s3://<your-bucket>/trusted/
s3://<your-bucket>/curated/
```

Upload the respective raw JSON files into each **landing** folder.

## 🧾 Glue SQL Table DDLs

Create the following tables manually in **AWS Glue Catalog** or run them through **Athena**:

- `ddl/customer_landing.sql`
- `ddl/accelerometer_landing.sql`
- `ddl/step_trainer_landing.sql`

Ensure all fields match their types (e.g., `FLOAT`, `STRING`) and the table locations point to the correct `landing` folders.

## 🔁 AWS Glue Jobs

Each script under `scripts/` represents a stage of the ETL pipeline:

| Script Name                          | Description                                                                 |
|-------------------------------------|-----------------------------------------------------------------------------|
| customer_landing_to_trusted.py     | Filters customer data for users who consented to research.                 |
| accelerometer_landing_to_trusted.py| Filters accelerometer data joined with trusted customers.                  |
| customer_trusted_to_curated.py     | Creates curated customer data linked to actual device usage.               |
| step_trainer_trusted.py            | Filters Step Trainer records linked to curated customers.                  |
| machine_learning_curated.py        | Joins step trainer and accelerometer data for ML training dataset.         |

Each script uses AWS Glue’s DynamicFrame API and must be submitted as a Glue Job with dynamic schema options enabled.

## 🔍 Athena Validation Queries

Run these queries and capture screenshots of the row counts:

### Landing Zone

```sql
SELECT COUNT(*) FROM customer_landing; -- Expected: 956
SELECT COUNT(*) FROM accelerometer_landing; -- Expected: 81273
SELECT COUNT(*) FROM step_trainer_landing; -- Expected: 28680
```

### Trusted Zone

```sql
SELECT COUNT(*) FROM customer_trusted; -- Expected: 482
SELECT COUNT(*) FROM accelerometer_trusted; -- ~40981
SELECT COUNT(*) FROM step_trainer_trusted; -- ~14460
```

### Curated Zone

```sql
SELECT COUNT(*) FROM customer_curated; -- Expected: 482 (or 464 if filtered)
SELECT COUNT(*) FROM machine_learning_curated; -- Expected: 43681 (or 34437 if filtered)
```

## 📸 Required Screenshots

Take and include these screenshots in the `/screenshots` folder:

1. Athena query for `customer_landing`
2. Athena query for `accelerometer_landing`
3. Athena query for `step_trainer_landing`
4. Athena query for `customer_trusted`
5. Athena query for `accelerometer_trusted`
6. Athena query for `step_trainer_trusted`
7. Athena query for `customer_curated`
8. Athena query for `machine_learning_curated`

## 🌟 Stand-Out Suggestions (Optional)

1. **Consent-based Filtering**: Only retain accelerometer data *after* the `shareWithResearchAsOfDate`.
2. **Anonymize Outputs**: Drop `email`, `name`, or PII fields from curated/ML outputs to ensure GDPR compliance.
3. **Add Partition Columns**: e.g., `ingestion_date` for future scalability.

## ✅ Submission Tips

- Replace `your-bucket` and `your_glue_db` in scripts before uploading.
- Ensure Athena tables and S3 paths are in sync.
- Validate record counts against expected values.
- Double-check your GitHub repo is public or shareable.

## 🧠 Author

Developed by a Data Engineer as part of the STEDI Step Trainer analytics platform for motion detection and human balance training.