from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="demo_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo"],
)
def demo_pipeline():

    @task
    def extract():
        print("Task A: dang lay du lieu...")
        return {"rows": 100}

    @task
    def transform(data: dict):
        print(f"Task B: xu ly {data['rows']} dong du lieu...")
        return {"rows_cleaned": data["rows"] - 5}

    @task
    def load(data: dict):
        print(f"Task C: nap {data['rows_cleaned']} dong vao kho du lieu.")

    load(transform(extract()))

demo_pipeline()
