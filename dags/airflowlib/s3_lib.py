from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


# simple download task
def download_file(bucket, key, destination):
    import boto3
    s3 = boto3.resource('s3')
    return s3.meta.client.download_file(bucket, key, destination)


# simple upload task
def upload_file(source, bucket, key):
    import boto3
    s3 = boto3.resource('s3')
    return s3.Bucket(bucket).upload_file(source, key)
