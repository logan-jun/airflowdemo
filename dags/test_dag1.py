import airflowlib.gcs_lib as gcs
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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
    return gcs.download_file_gcs(bucket='bsjun-test1', filename='temp_merged.csv', destination='/tempfiles/temp_merged.csv')

# Define the individual tasks using Python Operators
get_temperature_file = PythonOperator(
    task_id='get_temperature_file',
    python_callable=get_temperature_file,
    dag=dag)

remove_files = BashOperator(
    task_id='remove_files',
    bash_command='echo 1',
    dag=dag,
)

# construct the DAG by setting the dependencies
get_temperature_file >> remove_files
