
# Sparkify Data Pipeline and Analytics Dashboard

## Overview
The Sparkify Data Pipeline and Analytics Dashboard project builds a scalable Redshift-based data warehouse for Sparkify, a music streaming startup, enabling analysis of user activity and song metadata. The project includes an **ETL pipeline** that extracts raw data from S3, transforms it into a star schema in Redshift, and provides an **interactive dashboard** for analytical insights.

This project demonstrates how to design, implement, and deploy an end-to-end data pipeline with a focus on scalability, data quality, and user-friendly analytics.

---

## Project Features

### 1. ETL Pipeline
- Extracts JSON data (song and log files) from S3.
- Loads the data into **staging tables** in Redshift.
- Transforms and populates **fact** and **dimension tables** designed for analytical queries.
- Ensures data quality with advanced validation checks (e.g., referential integrity, null checks, duplicate checks).

### 2. Star Schema Design
The Redshift database schema is optimized for query performance and includes:
- **Fact Table**:
    - `songplays`: Records of song plays (joins user activity with song data).
- **Dimension Tables**:
    - `users`: App users (e.g., name, subscription level).
    - `songs`: Song metadata (e.g., title, duration).
    - `artists`: Artist information (e.g., name, location).
    - `time`: Timestamps broken down into components (e.g., hour, day, year).

### 3. Interactive Dashboard
- Built with **Streamlit**, the dashboard provides:
    - Real-time visualizations (e.g., bar and line charts) of analytical queries.
    - Advanced filtering options to customize views.
    - Auto-refresh functionality to display up-to-date insights.

---

## Requirements
Ensure you have the following before running the project:
- **AWS Redshift Cluster**:
    - Set up a cluster with an IAM role that has S3 read access.
- **Python 3.x**:
    - Install dependencies from `requirements.txt`.

---

## Setup Instructions

### 1. Clone the Repository
```bash
git clone <repository-url>
cd sparkify_project
```

### 2. Configure AWS and Database
Edit the `dwh.cfg` file with your Redshift and AWS credentials:
```plaintext
[CLUSTER]
HOST=<redshift-cluster-endpoint>
DB_NAME=sparkifydb
DB_USER=<username>
DB_PASSWORD=<password>
DB_PORT=5439

[IAM_ROLE]
ARN=<iam-role-arn>

[S3]
LOG_DATA=s3://udacity-dend/log_data
SONG_DATA=s3://udacity-dend/song_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
```

### 3. Install Dependencies
Install the required Python libraries:
```bash
pip install -r requirements.txt
```

### 4. Run the Application
Launch the pipeline and dashboard:
```bash
python main.py
```

---

## Project Components

### 1. Files
- **`dwh.cfg`**: Configuration file for AWS and Redshift credentials.
- **`sql_queries.py`**: SQL queries for creating, dropping, and populating tables.
- **`create_tables.py`**: Script to set up/reset the database schema.
- **`etl.py`**: Handles the ETL pipeline (extract, transform, load).
- **`analytics.py`**: Builds the Streamlit analytics dashboard.
- **`main.py`**: Single entry point to run the entire application.

### 2. ETL Pipeline Steps
1. **Staging**:
    - Copies raw JSON data from S3 into staging tables in Redshift.
2. **Transformation**:
    - Transforms and cleans data (e.g., handling nulls, joining datasets).
3. **Loading**:
    - Populates the `songplays` fact table and the `users`, `songs`, `artists`, and `time` dimension tables.

### 3. Analytics Dashboard
- Visualizes key metrics, including:
    - Top 10 most played songs.
    - User activity by subscription level.
    - Daily and yearly trends in user activity.
    - Most popular artists by play count.
- Provides a sidebar to filter queries and set refresh intervals.

---

## Example Queries

### 1. Top 10 Most Played Songs
```sql
SELECT s.title, COUNT(sp.songplay_id) AS play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 10;
```

### 2. User Activity by Day of the Week
```sql
SELECT t.weekday, COUNT(sp.songplay_id) AS activity_count
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.weekday
ORDER BY activity_count DESC;
```

---

## Notes
- **Security**: Ensure `dwh.cfg` is not included in public repositories to protect credentials.
- **Cleanup**: Delete your Redshift cluster and associated resources when not in use to avoid unnecessary costs.

---

## Future Enhancements
- Add support for larger datasets using Redshift Spectrum.
- Implement user authentication for the dashboard.
- Introduce machine learning models for predictive analytics.

