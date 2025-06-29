"""
Custom Airflow Operator: DataQualityOperator

Purpose:
This operator runs a series of SQL-based data quality tests on Redshift.
It executes each test, compares the result to an expected outcome, and fails
if any check does not match.

Each test is passed as a dictionary with:
- check_sql: SQL statement to run
- expected_result: value to compare against (int, string, or a callable like lambda)

Examples:
    tests = [
        {"check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM songs", "expected_result": lambda x: x > 0}
    ]
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on Redshift.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 tests: list,
                 *args, **kwargs):
        """
        Initializes the DataQualityOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param tests: List of dicts with 'check_sql' and 'expected_result'
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info("🚦 Running data quality checks...")

        if not self.tests:
            raise ValueError("❌ No tests provided to DataQualityOperator.")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failed_tests = 0

        for idx, test in enumerate(self.tests, start=1):
            sql = test.get('check_sql')
            expected = test.get('expected_result')

            if not sql:
                self.log.warning(f"⚠️ Skipping test {idx}: No SQL provided.")
                continue

            self.log.info(f"🔍 Test {idx}: Running SQL → {sql}")
            result = redshift.get_first(sql)

            if result is None or len(result) == 0:
                self.log.error(f"❌ Test {idx} failed: No result returned from query.")
                failed_tests += 1
                continue

            actual = result[0]
            self.log.info(f"✅ Test {idx} result: {actual} (Expected: {expected})")

            try:
                if callable(expected):
                    assert expected(actual), f"Callable expectation failed for test {idx}: got {actual}"
                else:
                    assert actual == expected, f"Expected {expected} but got {actual} for test {idx}"
            except AssertionError as ae:
                self.log.error(f"❌ Test {idx} FAILED: {ae}")
                failed_tests += 1

        if failed_tests > 0:
            raise ValueError(f"❌ {failed_tests} data quality checks failed.")
        else:
            self.log.info("🎉 All data quality checks passed!")
