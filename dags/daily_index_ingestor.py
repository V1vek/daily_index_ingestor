from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
from datetime import datetime
from datetime import timedelta

import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from lib import parser

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'daily_index_ingestor',
    default_args=default_args,
    description='Parse and store data',
    schedule_interval='0 0 * * 1-5'
)

ingest_raw_data = PythonOperator(
    task_id='ingest_raw_data',
    provide_context=True,
    python_callable=parser.ingest_raw,
    dag=dag,
)

aggregate_data = PythonOperator(
    task_id='aggregate_data',
    provide_context=True,
    python_callable=parser.aggregate_data,
    dag=dag,
)

ingest_raw_data >> aggregate_data
