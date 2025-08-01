import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# ADDED: Logging setup for the DAG file itself
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(project_root, 'src'))

from src.data_extractor.data_extractor import extract_data
from src.data_loader.data_loader import load_to_staging
from src.dwh_loader.dwh_loader import transform_and_load_to_dwh

# Get environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
API_KEY = os.getenv('API_KEY')
SYMBOL = os.getenv('SYMBOL')

# ADDED: Basic check for environment variables
if not all([API_KEY, SYMBOL, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    logger.error("One or more required environment variables are not set.")

DAG_ID = "financial-etl"
DWH_TABLES_DDL_SQL_PATH = os.path.join(os.getenv('AIRFLOW_HOME'), 'project', 'src', 'dwh_loader', 'sql', 'dwh_tables_ddl.sql')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['finance', 'pet']
) as dag:
    extract_data_to_csv = PythonOperator(
       task_id='extract_data_to_csv',
       python_callable=extract_data,
       op_kwargs={'api_key': API_KEY, 'symbol': SYMBOL},
    )

    create_staging_schema = PostgresOperator(
       task_id='create_staging_schema',
       postgres_conn_id='greenplum_conn',
       sql="CREATE SCHEMA IF NOT EXISTS staging;",
    )

    create_all_dwh_tables = PostgresOperator(
       task_id='create_all_dwh_tables',
       postgres_conn_id='greenplum_conn',
       sql=open(DWH_TABLES_DDL_SQL_PATH).read(),
    )

    load_data_task = PythonOperator(
       task_id='load_data_to_greenplum',
       python_callable=load_to_staging,
       provide_context=True,
    )

    transform_and_load_to_dwh_tables = PythonOperator(
       task_id='transform_and_load_to_dwh_tables',
       python_callable=transform_and_load_to_dwh,
       provide_context=True,
       op_kwargs={'symbol': SYMBOL},
    )

    extract_data_to_csv >> create_staging_schema >> create_all_dwh_tables >> \
    load_data_task >> transform_and_load_to_dwh_tables