"""
Custom Airflow Operator: LoadDimensionOperator

Purpose:
This operator loads data into a Redshift dimension table. It supports two modes:

1. 'truncate-insert' (default): deletes all existing data before inserting
2. 'append-only': simply appends new data

This pattern is common in star-schema modeling to ensure dimensions stay fresh
and deduplicated without requiring CDC (change data capture).

Usage:
    - Use in any dimension table DAG task
    - Leverages parameterized SQL inserts from a helper file
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadDimensionOperator(BaseOperator):
    """
    Loads data into a Redshift dimension table.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 sql_statement: str,
                 mode: str = 'truncate-insert',
                 *args, **kwargs):
        """
        Initializes the LoadDimensionOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param table: Dimension table name in Redshift
        :param sql_statement: SQL code that produces rows for the dimension table
        :param mode: Load mode: either 'truncate-insert' or 'append-only'
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.mode = mode.lower()

    def execute(self, context):
        self.log.info(f"🚀 Starting load for dimension table: {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode not in ['truncate-insert', 'append-only']:
            raise ValueError(f"Invalid mode: {self.mode}. Use 'truncate-insert' or 'append-only'.")

        if self.mode == 'truncate-insert':
            self.log.info(f"🧹 Truncating table before insert: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
        """

        self.log.info("📝 Executing SQL:\n%s", insert_sql)
        try:
            redshift.run(insert_sql)
            self.log.info(f"✅ Successfully loaded data into dimension table: {self.table}")
        except Exception as e:
            self.log.error(f"❌ Failed to load data into {self.table}: {e}")
            raise
