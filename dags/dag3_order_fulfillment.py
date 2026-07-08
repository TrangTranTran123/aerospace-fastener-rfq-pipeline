from airflow.sdk import dag, task
from airflow.providers.standard.sensors.python import PythonSensor
from datetime import datetime

def check_approval():
    from airflow.sdk import Variable
    status = Variable.get("quality_signoff_status", default_var="pending")
    print(f"Current approval status: {status}")
    return status == "approved"

@dag(
    dag_id="dag3_order_fulfillment",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["holyaero", "demo"],
)
def order_fulfillment():

    wait_for_approval = PythonSensor(
        task_id="wait_for_quality_signoff",
        python_callable=check_approval,
        poke_interval=10,
        timeout=300,
        mode="poke",
    )

    @task
    def decrement_stock():
        print("Decrementing ERPNext stock for fulfilled order...")

    @task
    def generate_invoice():
        print("Generating invoice PDF...")

    @task
    def notify_customer():
        print("Sending email notification to customer: 'Your order has shipped.'")

    wait_for_approval >> decrement_stock() >> generate_invoice() >> notify_customer()

order_fulfillment()
