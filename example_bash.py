from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
'example_bash',
start_date=datetime(2025,11,17),
schedule='@daily',
catchup=False
) as dag:
say_hello = BashOperator(
task_id='say_hello',
bash_command='echo "Hello from Airflow!"'
)
