from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def produce():
    return "airflow-data"

def consume(**context):
    msg = context['ti'].xcom_pull(task_ids='produce_task')
    print("Received:", msg)

with DAG(
    'htran_example_python_xcom',
    start_date=datetime(2025, 11, 17),
    schedule='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='produce_task', python_callable=produce)
    t2 = PythonOperator(task_id='consume_task', python_callable=consume)

    t1 >> t2
