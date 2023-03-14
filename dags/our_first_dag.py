from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'SaeedKhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v3',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 3, 10, 10),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first-task',
        bash_command="echo hello world! This is the task1"
    )

    task2 = BashOperator(
        task_id='second-task',
        bash_command="echo hello world! This is our second task run after task-1"
    )

    task3 = BashOperator(
        task_id='third-task',
        bash_command="echo hello world! This is our task-3 run alongside task-2"
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)