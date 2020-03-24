from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from datetime import datetime, timedelta

# simple download task
def download_file_gcs(bucket, filename, destination):
    storage_client = storage.Client.from_service_account_json('/root/airflow/auth/ds-data-platform-f859ac098f78.json')
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(filename)
    return blob.download_to_filename(destination)
