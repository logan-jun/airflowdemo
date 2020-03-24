import airflowlib.emr_lib as emr
import airflowlib.s3_lib as s3
import airflowlib.gcs_lib as gcs
import os
import pandas as pd
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
dag = DAG('beam_demo', concurrency=3, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='beam_cluster1', num_core_nodes=2)
    return cluster_id

# Creates a step in EMR cluster
def create_step(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    step_id = emr.add_job_flow_steps(jobflowId=cluster_id)
    return step_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

def wait_for_step(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    step_id = ti.xcom_pull(task_ids='add_emr_step')
    emr.wait_for_step_completion(cluster_id, step_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)

def get_dust_file_s3(**kwargs):
    file = s3.download_file_s3(bucket='bsjun-test1', key='before/data_merged.csv', destination='/tempfiles/data_merged.csv')
    return file

def upload_file_to_s3(**kwargs):
    return s3.upload_file_s3(source='/tempfiles/output.csv', bucket='bsjun-test1', key='before/output.csv')

def join_csv_files(**kwargs):
    a = pd.read_csv("/tempfiles/temp_merged.csv")
    b = pd.read_csv("/tempfiles/data_merged.csv")
    b = b.dropna(axis=1)
    merged = a.merge(b, on='dataTime')
    return merged.to_csv("/tempfiles/output.csv", index=False)

def get_temperature_file_gcs(**kwargs):
    return gcs.download_file_gcs(bucket='bsjun-test1', filename='temp_merged.csv', destination='/tempfiles/temp_merged.csv')

# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

add_emr_step = PythonOperator(
    task_id='add_emr_step',
    python_callable=create_step,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

wait_for_step_completion = PythonOperator(
    task_id='wait_for_step_completion',
    python_callable=wait_for_step,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

get_temperature_file_gcs = PythonOperator(
    task_id='get_temperature_file_gcs',
    python_callable=get_temperature_file_gcs,
    dag=dag)

upload_file_to_s3 = PythonOperator(
    task_id='upload_file_to_s3',
    python_callable=upload_file_to_s3,
    dag=dag)

get_dust_file_s3 = PythonOperator(
    task_id='get_dust_file_s3',
    python_callable=get_dust_file_s3,
    dag=dag)

join_csv_files = PythonOperator(
    task_id='join_csv_files',
    python_callable=join_csv_files,
    dag=dag)

remove_files = BashOperator(
    task_id='remove_files',
    bash_command='rm -f /tempfiles/data_merged.csv && rm -f /tempfiles/temp_merged.csv && rm -f /tempfiles/output.csv',
    dag=dag,
)

# construct the DAG by setting the dependencies
get_temperature_file_gcs >> join_csv_files
get_dust_file_s3 >> join_csv_files
join_csv_files >> upload_file_to_s3 >> remove_files
upload_file_to_s3 >> create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> add_emr_step >> wait_for_step_completion >> terminate_cluster
