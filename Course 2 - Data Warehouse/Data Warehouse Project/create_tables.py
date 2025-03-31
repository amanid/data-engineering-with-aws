import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

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
            "host={} dbname={} user={} password={} port={}".format(
                *config['CLUSTER'].values()
            )
        )
        cur = conn.cursor()
        logging.info("Connected to Redshift successfully.")
        return conn, cur
    except Exception as e:
        logging.error(f"Error connecting to Redshift: {e}")
        raise


def drop_tables(cur, conn):
    """
    Drop all existing tables in Redshift.
    """
    logging.info("Dropping existing tables.")
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed drop query: {query}")
        except Exception as e:
            logging.error(f"Error dropping table: {e}")


def create_tables(cur, conn):
    """
    Create all required tables in Redshift.
    """
    logging.info("Creating tables.")
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed create query: {query}")
        except Exception as e:
            logging.error(f"Error creating table: {e}")


def main():
    conn, cur = connect_to_redshift()

    try:
        drop_tables(cur, conn)
        create_tables(cur, conn)
    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
