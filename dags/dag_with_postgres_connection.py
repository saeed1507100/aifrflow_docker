from datetime import datetime, timedelta
import psycopg2

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'SaeedKhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

CREATE_TABLE_STATEMENT = """
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """


def get_sklearn():
    import sklearn
    print(f"scikit-learn version: {sklearn.__version__}")


def create_table():
    connection = psycopg2.connect(
        host="db.evzquwutdsfcxeaeenqm.supabase.co",
        database="postgres",
        port="5432",
        user="postgres",
        password="DataIntegrationFramework")

    cursor = connection.cursor()

    # Execute a query and get data
    cursor.execute(CREATE_TABLE_STATEMENT)
    print("Table created successfully")


with DAG(
    dag_id='dag_with_postgres_connection_v2',
    default_args=default_args,
    description='Cron trying out.',
    start_date=datetime(2023, 2, 1, 10),
    schedule_interval='0 3 * * Mon,Tue',
) as dag:
    task3 = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )

    task1 = PostgresOperator(
        task_id='create-table',
        postgres_conn_id='supabase_postgres_db',
        sql=CREATE_TABLE_STATEMENT
    )

    task2 = PythonOperator(
        task_id='create_table_with_psycopg2',
        python_callable=create_table
    )

    task3 >> [task1, task2]
