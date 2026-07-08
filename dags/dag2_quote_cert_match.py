from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="dag2_quote_cert_match",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["holyaero", "demo"],
)
def quote_cert_match():

    @task
    def match_certificate():
        print("Matching Certificate of Conformance to exact lot number...")
        cert_info = {"lot": "LOT-2026-0702", "cert_id": "COC-88213", "status": "matched"}
        print(f"  Result: {cert_info}")
        return cert_info

    @task
    def request_quality_signoff(cert_info: dict):
        from airflow.sdk import Variable
        Variable.set("quality_signoff_status", "pending")
        print("=" * 50)
        print(f"QUALITY SIGN-OFF REQUIRED for {cert_info['lot']}")
        print("Go to Airflow UI -> Admin -> Variables")
        print("Set 'quality_signoff_status' to 'approved' to proceed")
        print("=" * 50)

    cert = match_certificate()
    request_quality_signoff(cert)

quote_cert_match()
