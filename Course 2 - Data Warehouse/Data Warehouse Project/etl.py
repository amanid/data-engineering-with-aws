import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def connect_to_redshift():
    """
    Connect to the Redshift database using configuration from dwh.cfg.
    Returns:
        conn: psycopg2 connection object.
        cur: psycopg2 cursor object.
    """
    try:
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        conn = psycopg2.connect(
            f"host={config['CLUSTER']['HOST']} dbname={config['CLUSTER']['DB_NAME']} "
            f"user={config['CLUSTER']['DB_USER']} password={config['CLUSTER']['DB_PASSWORD']} "
            f"port={config['CLUSTER']['DB_PORT']}"
        )
        cur = conn.cursor()
        logging.info("Connected to Redshift successfully.")
        return conn, cur
    except Exception as e:
        logging.error(f"Error connecting to Redshift: {e}")
        raise


def load_staging_tables(cur, conn):
    """
    Load data from S3 into Redshift staging tables using COPY commands.
    """
    logging.info("Loading data into staging tables...")
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed COPY query: {query}")
        except Exception as e:
            logging.error(f"Error loading data into staging table: {e}")
            conn.rollback()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into fact and dimension tables.
    """
    logging.info("Inserting data into final tables...")
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed INSERT query: {query}")
        except Exception as e:
            logging.error(f"Error inserting data into final table: {e}")
            conn.rollback()


def run_advanced_data_quality_checks(cur):
    """
    Perform advanced data quality checks on fact and dimension tables.
    """
    logging.info("Running advanced data quality checks...")

    # Define quality checks
    quality_checks = [
        # Record count checks
        {"query": "SELECT COUNT(*) FROM songplays;",
         "description": "Check if `songplays` table is populated."},
        {"query": "SELECT COUNT(*) FROM users;",
         "description": "Check if `users` table is populated."},
        {"query": "SELECT COUNT(*) FROM songs;",
         "description": "Check if `songs` table is populated."},
        {"query": "SELECT COUNT(*) FROM artists;",
         "description": "Check if `artists` table is populated."},
        {"query": "SELECT COUNT(*) FROM time;",
         "description": "Check if `time` table is populated."},

        # NULL value checks
        {"query": "SELECT COUNT(*) FROM users WHERE user_id IS NULL;",
         "description": "Check for NULL `user_id` in `users` table."},
        {"query": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL;",
         "description": "Check for NULL `song_id` in `songs` table."},
        {"query": "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL;",
         "description": "Check for NULL `artist_id` in `artists` table."},

        # Duplicate checks
        {"query": "SELECT user_id, COUNT(*) FROM users GROUP BY user_id HAVING COUNT(*) > 1;",
         "description": "Check for duplicate `user_id` in `users` table."},
        {"query": "SELECT song_id, COUNT(*) FROM songs GROUP BY song_id HAVING COUNT(*) > 1;",
         "description": "Check for duplicate `song_id` in `songs` table."},

        # Referential integrity checks
        {"query": """
            SELECT COUNT(*)
            FROM songplays sp
            LEFT JOIN songs s ON sp.song_id = s.song_id
            WHERE sp.song_id IS NOT NULL AND s.song_id IS NULL;
            """,
         "description": "Check for unmatched `song_id` in `songplays` table."},

        {"query": """
            SELECT COUNT(*)
            FROM songplays sp
            LEFT JOIN artists a ON sp.artist_id = a.artist_id
            WHERE sp.artist_id IS NOT NULL AND a.artist_id IS NULL;
            """,
         "description": "Check for unmatched `artist_id` in `songplays` table."},
    ]

    # Execute quality checks
    for check in quality_checks:
        try:
            cur.execute(check["query"])
            result = cur.fetchone()
            if result[0] == 0:
                logging.error(f"Data quality check failed: {check['description']}")
                raise ValueError(f"Data quality check failed: {check['description']}")
            logging.info(f"Data quality check passed: {check['description']}")
        except Exception as e:
            logging.error(f"Error running data quality check: {e}")
            raise


def main():
    """
    Main function to orchestrate the ETL pipeline.
    Connects to Redshift, loads staging tables, transforms data, and performs quality checks.
    """
    conn, cur = connect_to_redshift()

    try:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        run_advanced_data_quality_checks(cur)  # Advanced quality checks
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
