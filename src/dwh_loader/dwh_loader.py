import os
import logging
from sqlalchemy import engine, create_engine

logger = logging.getLogger(__name__)


def read_sql_file(filepath):
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"SQL-fike not found:{filepath}")
        raise
    except Exception as e:
        logger.error(f"Error to read SQL-file {filepath: {e}}")
        raise


def transform_and_load_to_dwh(**kwargs):
    db_user = os.getenv('DB_USER')
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    engine_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(engine_string)

    sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

    try:
        with engine.connect() as conn:
            dim_date_sql = read_sql_file(os.path.join(sql_dir, 'dim_date_load.sql'))
            logger.info("SQL execution for loading dim_date...")
            conn.execute(dim_date_sql)
            logger.info("public.dim_date was update.")

            dim_symbol_sql = read_sql_file(os.path.join(sql_dir, 'dim_symbol_load.sql'))
            logger.info("SQL execution for loading dim_symbol.")
            conn.execute(dim_symbol_sql)
            logger.info("public.dim_symbol was update.")

            fact_stock_prices_sql = read_sql_file((os.path.join(sql_dir, 'fact_stock_prices_load.sql')))
            logger.info("SQL execution for loading fact_stock_prices...")
            conn.execute(fact_stock_prices_sql)
            logger.info("public.fact_stock_prices was update")

        logger.info("All data was load and transform in DWH-tables.")
    except Exception as e:
        logger.error(f"Error during DWH transform: {e}")
        raise
