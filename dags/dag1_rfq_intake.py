from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(
    dag_id="dag1_rfq_intake",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["holyaero", "demo"],
)
def rfq_intake():

    @task
    def parse_rfq():
        print("Parsing RFQ list from customer email...")
        rfq_items = [
            {"part_number": "NAS1351-08", "qty": 500},
            {"part_number": "MS21042-08", "qty": 200},
        ]
        print(f"Parsed {len(rfq_items)} line items")
        return rfq_items

    @task
    def match_erpnext_stock(rfq_items: list):
        print("Matching against ERPNext stock levels...")
        erp_stock = {"NAS1351-08": 800, "MS21042-08": 150}
        for item in rfq_items:
            available = erp_stock.get(item["part_number"], 0)
            status = "OK" if available >= item["qty"] else "SHORT"
            print(f"  {item['part_number']}: need {item['qty']}, have {available} -> {status}")

    parsed = parse_rfq()
    match_stock = match_erpnext_stock(parsed)

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_quote_cert_match",
        trigger_dag_id="dag2_quote_cert_match",
    )

    match_stock >> trigger_dag2

rfq_intake()
