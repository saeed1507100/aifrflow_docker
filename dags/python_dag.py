from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'SaeedKhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def greet(age, ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    print(f"Hello world! My Name is {first_name} {last_name}, and I am {age} years old!")


def get_name(ti):
    ti.xcom_push(key='first_name', value='Saeed Anwar')
    ti.xcom_push(key='last_name', value='Khan')


with DAG(
        dag_id='dag_with_python_operator_v5',
        default_args=default_args,
        description='This is our first dag with python operator',
        start_date=datetime(2023, 3, 13, 10),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task2 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age': '26'}
    )

    task1 >> task2
