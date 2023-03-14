from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'SaeedKhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    dag_id='dag-with_taskflow_api_v2',
    default_args=default_args,
    start_date=datetime(2023, 3, 13),
    schedule_interval='@daily'
)
def hello_world_etl():
    @task()
    def get_age():
        return 19

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name': 'Saeed', 'last_name': 'Khan'}

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! I am {first_name} {last_name}, and I am {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(name_dict['first_name'], name_dict['last_name'], age)


greet_dag = hello_world_etl()
