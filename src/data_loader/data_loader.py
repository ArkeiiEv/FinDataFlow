import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()


def load_to_staging(**kwargs):
    ti = kwargs.get('ti')

    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        logger.error("Database environment variables are not fully set.")
        raise ValueError("Missing database environment variables.")

    file_path = ti.xcom_pull(task_ids='extract_data_to_csv', key='return_value')

    if not file_path or not os.path.exists(file_path):
        logger.warning(f"CSV file not found at path {file_path}. Skipping staging load.")
        raise ValueError(f"CSV file not found at path {file_path} for staging load.")

    logger.info(f"Attempting to load data from {file_path} into staging.")

    conn = None
    cur = None
    try:
        df = pd.read_csv(file_path)
        df.rename(columns={
            'timestamp': 'date',
            'open': 'open_price',
            'close': 'close_price'
        }, inplace=True)

        # --- НАЧАЛО ДИАГНОСТИЧЕСКИХ ЛОГОВ ---
        # ЭТИ СТРОКИ ДОЛЖНЫ ПОЯВИТЬСЯ В ВАШИХ ЛОГАХ
        logger.info(f"DataFrame columns BEFORE explicit selection: {df.columns.tolist()}")

        df = df[['date', 'open_price', 'high', 'low', 'close_price', 'volume', 'symbol']]
        logger.info(f"Read {len(df)} rows from CSV.")

        logger.info(f"DataFrame columns AFTER explicit selection: {df.columns.tolist()}")
        if not df.empty:
            logger.info(f"First row of DataFrame (as tuple): {tuple(df.iloc[0].values)}")
        else:
            logger.warning("DataFrame is empty, cannot log first row.")

        logger.info(f"Connecting to Greenplum at {DB_HOST}:{DB_PORT} as {DB_USER} to {DB_NAME}...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = False
        cur = conn.cursor()
        logger.info("Successfully connected to Greenplum.")

        logger.info("Truncating staging.stock_prices table to ensure a clean load.")
        cur.execute("TRUNCATE TABLE staging.stock_prices;")

        logger.info("Starting table existence check for 'staging.stock_prices'...")
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'staging' AND table_name = 'stock_prices'
            );
        """)
        table_exists = cur.fetchone()[0]
        if table_exists:
            logger.info(
                "'staging.stock_prices' table IS VISIBLE to this connection. Proceeding to load data using execute_batch.")
        else:
            logger.error("'staging.stock_prices' table IS NOT VISIBLE to this connection. This is the root cause.")
            raise psycopg2.errors.UndefinedTable("Relation 'staging.stock_prices' is not visible to this connection.")

        data_to_insert = [tuple(row) for row in df.values]

        # ЭТА ПРОВЕРКА УЖЕ ПРОХОДИЛА УСПЕШНО В ВАШИХ ПРЕДЫДУЩИХ ЛОГАХ
        expected_columns = 7
        for i, row_tuple in enumerate(data_to_insert):
            if len(row_tuple) != expected_columns:
                logger.error(
                    f"Row {i} has incorrect number of elements: {len(row_tuple)} instead of {expected_columns}. Tuple: {row_tuple}")
                raise ValueError(f"Mismatch in tuple length for INSERT at row {i}.")
        logger.info(f"All {len(data_to_insert)} tuples verified for correct length ({expected_columns} elements each).")

        insert_sql = """
            INSERT INTO staging.stock_prices 
            (date, open_price, high, low, close_price, volume, symbol) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        logger.info(f"Executing batch INSERT for {len(data_to_insert)} rows into 'staging.stock_prices'.")
        execute_batch(cur, insert_sql, data_to_insert, page_size=1000)

        conn.commit()
        logger.info(f"Loaded {len(data_to_insert)} rows into staging.stock_prices using execute_batch.")

    except psycopg2.Error as e:
        logger.error(f"Database error during staging load: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Staging load failed: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

    logger.info("This script is designed to be run via Airflow. Manual XCom setup is required for standalone testing.")