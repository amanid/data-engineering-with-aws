"""
DAG: sparkify_dag

This ETL pipeline loads song and user activity JSON logs from S3 into Amazon Redshift,
stages the data, populates fact and dimension tables, and runs data quality checks.

- Data Source: S3 bucket containing song and event logs in JSON format.
- Destination: Amazon Redshift (Serverless recommended).
- Execution: Hourly, supports backfill and retries.
- DAG Tasks: Stage → Fact → Dimensions → Data Quality Checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# -------------------------------
# DAG Default Configuration
# -------------------------------

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    catchup=False,  # No past backfilling unless manually triggered
    max_active_runs=1  # Avoid overlapping executions
)

# -------------------------------
# Constants
# -------------------------------

S3_BUCKET = 'your-bucket'
LOG_DATA_KEY = 'log-data'
SONG_DATA_KEY = 'song-data'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'

# -------------------------------
# DAG Tasks
# -------------------------------

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

# Stage Events JSON data to Redshift
stage_events = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key=LOG_DATA_KEY,
    json_path=LOG_JSON_PATH,
    region=REGION
)

# Stage Songs JSON data to Redshift
stage_songs = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=SONG_DATA_KEY,
    json_path='auto',  # Infer schema automatically
    region=REGION
)

# Load the main songplays fact table
load_songplays = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_statement=SqlQueries.songplay_table_insert
)

# Load user dimension table (with truncate-insert mode)
load_user_dim = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_statement=SqlQueries.user_table_insert,
    mode='truncate-insert'
)

# Load song dimension table
load_song_dim = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_statement=SqlQueries.song_table_insert,
    mode='truncate-insert'
)

# Load artist dimension table
load_artist_dim = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_statement=SqlQueries.artist_table_insert,
    mode='truncate-insert'
)

# Load time dimension table
load_time_dim = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_statement=SqlQueries.time_table_insert,
    mode='truncate-insert'
)

# Run data quality checks on critical tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=[
        {
            'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
            'expected_result': 0
        },
        {
            'check_sql': 'SELECT COUNT(*) FROM songs',
            'expected_result': lambda x: x > 0
        },
        {
            'check_sql': 'SELECT COUNT(DISTINCT start_time) FROM songplays',
            'expected_result': lambda x: x > 0
        }
    ]
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# -------------------------------
# DAG Task Dependencies
# -------------------------------

start_operator >> [stage_events, stage_songs]
[stage_events, stage_songs] >> load_songplays
load_songplays >> [load_user_dim, load_song_dim, load_artist_dim, load_time_dim]
[load_user_dim, load_song_dim, load_artist_dim, load_time_dim] >> run_quality_checks
run_quality_checks >> end_operator
