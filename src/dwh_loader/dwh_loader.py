import os
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def read_sql_file(filepath):
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"SQL-file not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Error reading SQL-file {filepath}: {e}")
        raise

def transform_and_load_to_dwh(**kwargs):
    symbol_to_load = kwargs.get('symbol')
    if not symbol_to_load:
        logger.error("Symbol not provided to transform_and_load_to_dwh function.")
        raise ValueError("Missing 'symbol' argument for DWH transformation.")

    db_user = os.getenv('DB_USER')
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    engine_instance = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

    try:
        with engine_instance.connect() as conn:
            dim_date_sql_content = read_sql_file(os.path.join(sql_dir, 'dim_date_load.sql'))
            logger.info("SQL execution for loading dim_date...")
            conn.execute(text(dim_date_sql_content))
            logger.info("public.dim_date was updated.")

            dim_symbol_sql_content = read_sql_file(os.path.join(sql_dir, 'dim_symbol_load.sql'))
            logger.info("SQL execution for loading dim_symbol.")
            conn.execute(text(dim_symbol_sql_content), {'symbol_code': symbol_to_load})
            logger.info("public.dim_symbol was updated.")

            fact_stock_prices_sql_content = read_sql_file((os.path.join(sql_dir, 'fact_stock_prices_load.sql')))
            logger.info("SQL execution for loading fact_stock_prices...")
            conn.execute(text(fact_stock_prices_sql_content), {'symbol_code': symbol_to_load})
            logger.info("public.fact_stock_prices was updated.")

        logger.info("All data was loaded and transformed in DWH-tables.")
    except Exception as e:
        logger.error(f"Error during DWH transform: {e}")
        raise