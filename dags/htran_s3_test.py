import boto3
import pandas as pd
import io
from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator

bucket_name = Variable.get("bucket_name")

def read():
	s3 = boto3.client(
	    's3',
	    aws_access_key_id=Variable.get("aws_access_key_id"),
	    aws_secret_access_key=Variable.get("aws_secret_access_key"),
	    region_name='us-east-1'
	)
	obj = s3.get_object(Bucket=bucket_name, Key='data/scores.csv')
	df_loaded = pd.read_csv(obj['Body'])
	print(df_loaded)

with DAG(
    dag_id="htran_s3_test",
    start_date=datetime(2025, 11, 17),
    schedule="0 * * * *",
    catchup=False
) as dag:

	check = PythonOperator(
		task_id='check', 
		python_callable=read
	)
