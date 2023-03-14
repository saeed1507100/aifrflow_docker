from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'SaeedKhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_v2',
    default_args=default_args,
    description='Catchup & Back-fill trying out.',
    start_date=datetime(2023, 3, 1, 10),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='first-task',
        bash_command="echo hello world! Date: {{ ds }}"
    )

    task1
