import pandas as pd
import psycopg2
import os
import logging

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')


def load_to_staging():
    conn = None
    cur = None
    try:
        raw_files = [f for f in os.listdir("/data/raw") if f.endswith('.csv')]
        if not raw_files:
            logging.warning("No raw CSV files found in /data/raw to load.")
            return

        latest_file = sorted(raw_files)[-1]
        file_path = f"/data/raw/{latest_file}"
        logging.info(f"Attempting to load data from {file_path}")

        df = pd.read_csv(file_path)
        df.rename(columns={
            'timestamp': 'date',
            'open': 'open_price',
            'close': 'close_price'
        }, inplace=True)
        df = df[['date', 'open_price', 'high', 'low', 'close_price', 'volume']]
        logging.info(f"Read {len(df)} rows from CSV.")

        logging.info(f"Connecting to Greenplum at {DB_HOST}:{DB_PORT} as {DB_USER} to {DB_NAME}...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        #### conn.autocommit = True # Важно для DDL (CREATE SCHEMA/TABLE)

        cur = conn.cursor()
        logging.info("Successfully connected to Greenplum.")

        logging.info("Ensuring 'staging' schema and 'stock_prices' table exist...")
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS staging.stock_prices (
                    date DATE,
                    open_price NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close_price NUMERIC,
                    volume BIGINT
                );
            """)
            logging.info("Schema 'staging' and table 'stock_prices' checked/created successfully.")
        except psycopg2.Error as create_error:
            logging.error(f"Error creating schema or table: {create_error}")
            return

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO staging.stock_prices
                (date, open_price, high, low, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

        conn.commit()
        logging.info(f"Loaded {len(df)} rows to staging.stock_prices")
    except psycopg2.Error as e:
        logging.error(f"Database error during loading: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"Loading failed: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    load_to_staging()