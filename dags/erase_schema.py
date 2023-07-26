from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

postgres_connection_name = "postgres_localhost" # Established connection with Airflow
schema_name = "public" # Desired schema name

default_dag_args = {
    "owner": "George Linstein",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="erase_schema",
    default_args=default_dag_args,
    description="Erase schema",
    start_date=datetime(2023, 1, 10, 0),
    schedule_interval="@once",
) as dag:

    #Erase current schema
    erase_schema = PostgresOperator(
        task_id="erase_create_schema",
        postgres_conn_id=postgres_connection_name,
        sql=f"DROP SCHEMA {schema_name} CASCADE;"
            f"CREATE SCHEMA {schema_name};"

    )

    erase_schema