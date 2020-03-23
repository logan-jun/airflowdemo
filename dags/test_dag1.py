import airflowlib.s3_lib as s3
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

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
def get_temperature_file(**kwargs):
    file = s3.download_file(bucket='bsjun-test1', key='before/temp_merged.csv', destination='/tempfiles/temp_merged.csv')
    return file

def get_dust_file(**kwargs):
    file = s3.download_file(bucket='bsjun-test1', key='before/data_merged.csv', destination='/tempfiles/data_merged.csv')
    return file

# Define the individual tasks using Python Operators
get_temperature_file = PythonOperator(
    task_id='get_temperature_file',
    python_callable=get_temperature_file,
    dag=dag)

get_dust_file = PythonOperator(
    task_id='get_dust_file',
    python_callable=get_dust_file,
    dag=dag)

# construct the DAG by setting the dependencies
get_temperature_file >> get_dust_file
