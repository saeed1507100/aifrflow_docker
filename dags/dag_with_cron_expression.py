from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'SaeedKhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_cron_v1',
    default_args=default_args,
    description='Cron trying out.',
    start_date=datetime(2023, 2, 1, 10),
    schedule_interval='0 3 * * Mon,Tue',
) as dag:
    task1 = BashOperator(
        task_id='cron-task',
        bash_command="echo hello world! Date: {{ ds }}"
    )

    task1
