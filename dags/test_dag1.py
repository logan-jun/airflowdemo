import airflowlib.gcs_lib as gcs
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 01),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('test_dag1', concurrency=3, schedule_interval=None, default_args=default_args)

# Creates an EMR cluster
s3_to_redshift = S3ToRedshiftOperator(
  task_id="s3_to_redshift",
  redshift_conn_id="my_redshift",
  aws_conn_id="my_conn_s3",
  table="result",
  s3_bucket="bsjun-test1",
  s3_key="output/result-00000-of-00002.csv",
  verify=True,
  dag=dag
)
# Define the individual tasks using Python Operators

remove_files = BashOperator(
    task_id='remove_files',
    bash_command='echo 1',
    dag=dag,
)

# construct the DAG by setting the dependencies
s3_to_redshift >> remove_files
