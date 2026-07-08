from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'htran_example_parallel',
    start_date=datetime(2025, 11, 17),
    schedule='@daily',
    catchup=False
) as dag:

    prepare = BashOperator(task_id='prepare', bash_command='echo "Preparing"')
    task_a = BashOperator(task_id='task_a', bash_command='echo "A"')
    task_b = BashOperator(task_id='task_b', bash_command='echo "B"')
    finish = BashOperator(task_id='finish', bash_command='echo "Done"')

    prepare >> [task_a, task_b] >> finish
