import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(project_root, 'src'))

from src.data_extractor.data_extractor import extract_data
from src.data_loader.data_loader import load_to_staging

DAG_ID = "financial-etl"

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2025, 7, 29),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
	'schedule_interval': '@daily',
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=default_args['start_date'],
    schedule='@daily',
    catchup=False,
    tags=['finance', 'pet']
) as dag:
	extract_data_task = PythonOperator(
		task_id='extract_data_to_csv',
		python_callable=extract_data,
	)

	load_data_task = PythonOperator(
		task_id='load_data_to_greenplum',
		python_callable=load_to_staging,
	)

	extract_data_task >> load_data_task