from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import json

# ---------------------------
# Configuration - REPLACE 'your_name' with your actual name
# ---------------------------
STUDENT_NAME = "htran"
S3_BUCKET = "s3-cloud-computing-env"
RAW_PREFIX = f"students/{STUDENT_NAME}/raw/60sec"
AGG_PREFIX = f"students/{STUDENT_NAME}/agg"

s3 = boto3.client(
    's3',
    aws_access_key_id='YOUR_AWS_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_AWS_SECRET_ACCESS_KEY,
    region_name='us-east-1'
)


# ---------------------------
# Helper: Load raw files from S3
# ---------------------------
def load_raw_files(start_dt, end_dt):
    """Load raw JSON files from S3 for [start_dt, end_dt). Return list of dicts."""
    records = []
    t = start_dt

    while t < end_dt:
        key = f"{RAW_PREFIX}/{t:%Y/%m/%d/%H/%M}.json"

        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            content = obj["Body"].read()
            items = json.loads(content)

            if isinstance(items, list):
                records.extend(items)
            else:
                records.append(items)

        except s3.exceptions.NoSuchKey:
            pass
        except Exception as e:
            print(f"Error loading {key}: {e}")

        t += timedelta(minutes=1)

    return records


# ---------------------------
# Helper: Write merged data to S3
# ---------------------------
def write_json_to_s3(prefix, dt, records):
    """Write merged raw records to S3 in JSON format."""
    key = f"{prefix}/{dt:%Y/%m/%d/%H/%M}.json"
    json_bytes = json.dumps(records).encode("utf-8")

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json_bytes,
        ContentType="application/json"
    )

    print(f"Uploaded JSON: s3://{S3_BUCKET}/{key}")


# ---------------------------
# Aggregation function
# ---------------------------
def aggregate_window(window_minutes, **context):
    """Merge raw JSON data for the specified time window and write to S3."""
    exec_dt = context["logical_date"]
    window_start = exec_dt - timedelta(minutes=window_minutes)

    print(f"Processing {window_minutes}-minute window: {window_start} → {exec_dt}")

    records = load_raw_files(window_start, exec_dt)

    if not records:
        print(f"No raw data for {window_minutes}-minute window.")
        return

    write_json_to_s3(f"{AGG_PREFIX}/{window_minutes}min", exec_dt, records)


# ---------------------------
# DAG Definition
# ---------------------------
with DAG(
    dag_id="multi_window_raw_merge_htran",
    start_date=datetime(2025, 11, 17),
    schedule="* * * * *",  # Every minute
    catchup=False,
) as dag:

    # ========== 5-Minute Window ==========
    def is_5min(**context):
        return context["logical_date"].minute % 5 == 0

    check_5min = ShortCircuitOperator(
        task_id="check_5min",
        python_callable=is_5min,
    )

    agg_5min = PythonOperator(
        task_id="agg_5min",
        python_callable=aggregate_window,
        op_kwargs={"window_minutes": 5},
    )

    check_5min >> agg_5min

    # ========== 10-Minute Window ==========
    def is_10min(**context):
        return context["logical_date"].minute % 10 == 0

    check_10min = ShortCircuitOperator(
        task_id="check_10min",
        python_callable=is_10min,
    )

    agg_10min = PythonOperator(
        task_id="agg_10min",
        python_callable=aggregate_window,
        op_kwargs={"window_minutes": 10},
    )

    check_10min >> agg_10min

    # ========== 20-Minute Window ==========
    def is_20min(**context):
        return context["logical_date"].minute % 20 == 0

    check_20min = ShortCircuitOperator(
        task_id="check_20min",
        python_callable=is_20min,
    )

    agg_20min = PythonOperator(
        task_id="agg_20min",
        python_callable=aggregate_window,
        op_kwargs={"window_minutes": 20},
    )

    check_20min >> agg_20min
