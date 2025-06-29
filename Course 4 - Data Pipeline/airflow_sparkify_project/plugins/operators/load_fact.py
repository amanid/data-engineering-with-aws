"""
Custom Airflow Operator: LoadFactOperator

Purpose:
This operator loads data into a Redshift fact table by executing a SQL statement.
The SQL statement is typically defined in a helper module and should return
records to be appended to the fact table.

Usage:
    - Designed for append-only loads
    - Executes parameterized SQL inserts into the target fact table
    - Logs all operations for auditability
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadFactOperator(BaseOperator):
    """
    Load data into a Redshift fact table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 sql_statement: str,
                 *args, **kwargs):
        """
        Initializes the LoadFactOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param table: Redshift fact table to load into
        :param sql_statement: SQL insert statement (without the INSERT INTO part)
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info(f"📌 Starting to load fact table: {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
        """

        self.log.info("📝 Executing SQL:\n%s", insert_sql)
        try:
            redshift.run(insert_sql)
            self.log.info(f"✅ Data successfully inserted into fact table: {self.table}")
        except Exception as e:
            self.log.error(f"❌ Failed to insert data into fact table {self.table}: {e}")
            raise
