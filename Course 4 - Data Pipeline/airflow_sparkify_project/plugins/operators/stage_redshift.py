"""
Custom Airflow Operator: StageToRedshiftOperator

Purpose:
This operator stages JSON data from Amazon S3 to a staging table in Amazon Redshift
using a dynamically constructed COPY command. It supports JSONPath loading and can be
templated for dynamic execution dates, making it suitable for backfill and hourly ingestion.

Usage:
    - Supports loading `log-data` or `song-data` into staging tables.
    - Configurable S3 key supports Jinja templating (e.g., {{ ds }})
    - Assumes Redshift tables are pre-created.
"""

from typing import Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)  # Allows for dynamic templating (e.g., {{ execution_date.strftime('%Y/%m/%d') }})

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_credentials_id: str,
                 table: str,
                 s3_bucket: str,
                 s3_key: str,
                 region: str = 'us-west-2',
                 json_path: Optional[str] = 'auto',
                 *args, **kwargs):
        """
        Initializes the StageToRedshiftOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param aws_credentials_id: Airflow connection ID for AWS credentials
        :param table: Redshift target table to COPY into
        :param s3_bucket: S3 bucket where source data is located
        :param s3_key: S3 object key (can be templated)
        :param region: AWS region of S3 bucket
        :param json_path: Optional S3 URI for JSONPath file; use 'auto' for automatic mapping
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        self.log.info("🔄 Starting StageToRedshiftOperator for table: %s", self.table)

        # Resolve credentials
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = s3_hook.get_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key

        # Resolve rendered S3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info("📥 Copying data from: %s", s3_path)

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear staging table
        self.log.info("🧹 Clearing data from Redshift staging table: %s", self.table)
        redshift.run(f"DELETE FROM {self.table}")

        # Prepare COPY SQL
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{access_key}'
            SECRET_ACCESS_KEY '{secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}';
        """

        self.log.info("🚀 Executing COPY command into Redshift...")
        try:
            redshift.run(copy_sql)
            self.log.info("✅ COPY command executed successfully for table: %s", self.table)
        except Exception as e:
            self.log.error("❌ COPY command failed: %s", str(e))
            raise
