"""
Airflow Utility Operator: LoadRedshiftOperator

Purpose:
    Executes any SQL command(s) against Amazon Redshift. Useful for DDL/DML operations
    such as creating schemas, cleaning tables, or preparing staging environments.

Features:
    - Supports multi-line SQL commands.
    - SQL can be fully templated using Airflow context (e.g., {{ ds }}, {{ execution_date }}).
    - Raises error if the SQL execution fails or is empty.
    - Integrates with Redshift via PostgresHook.

Example Usage:
    create_schema = LoadRedshiftOperator(
        task_id='create_staging_schema',
        redshift_conn_id='redshift',
        sql=\"\"\"
            CREATE SCHEMA IF NOT EXISTS staging;
        \"\"\"
    )
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadRedshiftOperator(BaseOperator):
    """
    Generic operator to execute raw SQL on Redshift.
    """

    ui_color = '#B0BEC5'
    template_fields = ('sql',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 sql: str,
                 *args, **kwargs):
        """
        Initialize LoadRedshiftOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param sql: The SQL statement to execute (can be multi-line and templated)
        """
        super(LoadRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        if not self.sql or not self.sql.strip():
            raise ValueError("❌ No SQL command provided to LoadRedshiftOperator.")

        self.log.info("📡 Connecting to Redshift: '%s'", self.redshift_conn_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("📝 Executing the following SQL on Redshift:\n%s", self.sql)

        try:
            redshift.run(self.sql)
            self.log.info("✅ SQL executed successfully in Redshift.")
        except Exception as e:
            self.log.error("❌ SQL execution failed: %s", str(e))
            raise
