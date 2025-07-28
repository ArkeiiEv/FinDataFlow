from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Ваш остальной код DAG
def my_callable_function():
    print("Привет из PythonOperator!")

with DAG(
    dag_id='my_example_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:
    run_python_task = PythonOperator(
        task_id='run_my_python_callable',
        python_callable=my_callable_function,
    )