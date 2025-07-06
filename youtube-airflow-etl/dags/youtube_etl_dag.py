from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
from batch_etl import batch_run

default_args = {
    'owner': 'yonatan',
    'start_date': datetime(2025, 6, 29),
    'retries': 1
}

with DAG(
    dag_id='youtube_kpi_daily_batch',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_batch_etl = PythonOperator(
        task_id='run_batch_youtube_etl',
        python_callable=batch_run
    )

    run_batch_etl