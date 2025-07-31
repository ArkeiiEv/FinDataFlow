import pandas as pd
import psycopg2
import os
import logging

from dotenv import load_dotenv
from multipart import file_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')


def load_to_staging(**kwargs):

    ti = kwargs.get('ti')
    file_path = ti.xcom_pull(task_ids = 'extract_data_to_csv', key = 'return_value')

    if not file_path or not os.path.exists(file_path):
        logging.warning(f"CSV file not found at path {file_path}. Skipping staging load.")
        raise ValueError(f"CSV file not found at path {file_path} for staging load.")

    logging.info(f"Attempting to load data from {file_path} into staging.")

    conn = None
    cur = None
    try:
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
        conn.autocommit = False

        cur = conn.cursor()
        logging.info("Successfully connected to Greenplum.")
        temp_csv_file = file_path + "_temp_copy.csv"
        df.to_csv(temp_csv_file, index=False, header=False)

        with open(temp_csv_file, 'r') as f:
            cur.copy_from(f, 'staging.stock_prices', sep=',',
                          columns=('date', 'open_price', 'high', 'low', 'close_price', 'volume'))

        conn.commit()
        logging.info(f"Loaded {len(df)} rows into staging.stock_prices using COPY FROM.")

        os.remove(temp_csv_file)
        logging.info(f"Removed temporary file: {temp_csv_file}")

    except psycopg2.Error as e:
        logging.error(f"Database error during staging load: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"Staging load failed: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Database connection closed.")

    if __name__ == "__main__":
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

        logging.info(
            "This script is designed to be run via Airflow. Manual XCom setup is required for standalone testing.")