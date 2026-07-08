from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'htran_example_dependency',
    start_date=datetime(2025, 11, 17),
    schedule='@hourly',
    catchup=False
) as dag:

    start = BashOperator(task_id='start', bash_command='echo "Start"')
    middle = BashOperator(task_id='middle', bash_command='echo "Middle"')
    end = BashOperator(task_id='end', bash_command='echo "End"')

    start >> middle >> end
