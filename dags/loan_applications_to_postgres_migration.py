from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

from includes.loanoffers_table import read_mongodb_loanoffers_data, write_loanoffers_date_to_SQL
from includes.loanapplication_table import read_mongodb_loanapplications_data, write_loanapplications_date_to_SQL
from includes.loanproducts_table import read_mongodb_loanproducts_data, write_loanproducts_date_to_SQL
from includes.mlscore_table import read_mongodb_mlscore_data, write_mlscore_date_to_SQL
from includes.loandeals_table import read_mongodb_loandeals_data, write_loandeals_date_to_SQL

postgres_connection_name = "postgres_localhost"
schema_name = "public"

# DAG settings

default_dag_args = {
    "owner": "George Linstein",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# Create DAG instanse

with DAG(
    dag_id="loan_applications_migration",
    default_args=default_dag_args,
    description="Create table => Read data from Mongo => Write data to postgres SQL",
    start_date=datetime(2023, 2, 7, 10),
    schedule_interval="00 02 * * *",
) as dag:

     # Create SQL tables
    create_loanoffers_table_task = PostgresOperator(
        task_id="create_loanoffers_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.loanoffers"
            "(id serial PRIMARY KEY, _id varchar UNIQUE, financedAmount decimal, period integer, minOffer decimal, optOffer decimal);"
    )

    create_loanproducts_table_task = PostgresOperator(
        task_id="create_loanproducts_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.loanproducts"
            "(id serial PRIMARY KEY, _id varchar UNIQUE, name varchar, productType varchar, type varchar, sellersType varchar, totalBuyingPrice decimal);"
    )

    create_loanapplications_table_task = PostgresOperator(
        task_id="create_loanapplication_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.loanapplications"
            "(id serial PRIMARY KEY, _id varchar, deleted boolean, dateCreated date, name varchar, email varchar, phoneNumber varchar, status varchar, assignee varchar, products varchar UNIQUE, dealId varchar);"
    )

    create_mlscore_table_task = PostgresOperator(
        task_id="create_mlscore_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.mlscore"
            "(id serial PRIMARY KEY, _id varchar UNIQUE, loanId varchar, score decimal, categoriesTotalScore decimal, dateCreated date);"
    )

    create_loandeals_table_task = PostgresOperator(
        task_id="create_loandeals_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.loandeals"
            "(id serial PRIMARY KEY, _id varchar UNIQUE, minOffer decimal, totalBuying decimal, periodWeeks decimal, deleted boolean);"
    )

    # Read mongoDB data
    read_mongodb_loanoffers_data_task = PythonOperator(
        task_id="read_loanoffers_table",
        python_callable=read_mongodb_loanoffers_data
    )
    read_mongodb_loanproducts_data_task = PythonOperator(
        task_id="read_loanproducts_table",
        python_callable=read_mongodb_loanproducts_data
    )
    read_mongodb_loanapplications_data_task = PythonOperator(
        task_id="read_loanapplications_table",
        python_callable=read_mongodb_loanapplications_data
    )
    read_mongodb_mlscore_data_task = PythonOperator(
        task_id="read_mlscore_table",
        python_callable=read_mongodb_mlscore_data
    )

    read_mongodb_loandeals_data_task = PythonOperator(
        task_id="read_loandeals_table",
        python_callable=read_mongodb_loandeals_data
    )
    
    # Read mongoDB data
    write_loanoffers_data_task = PythonOperator(
        task_id="write_loanoffers_table",
        python_callable=write_loanoffers_date_to_SQL,
        op_args={schema_name}
    )
    write_loanproducts_data_task = PythonOperator(
        task_id="write_loanproducts_table",
        python_callable=write_loanproducts_date_to_SQL,
        op_args={schema_name}
    )
    write_loanapplications_data_task = PythonOperator(
        task_id="write_loanapplications_table",
        python_callable=write_loanapplications_date_to_SQL,
        op_args={schema_name}
    )
    write_mlscore_data_task = PythonOperator(
        task_id="write_mlscore_table",
        python_callable=write_mlscore_date_to_SQL,
        op_args={schema_name}
    )
    write_loandeals_data_task = PythonOperator(
        task_id="write_loandeals_table",
        python_callable=write_loandeals_date_to_SQL,
        op_args={schema_name}
    )
    
    create_loanoffers_table_task >> read_mongodb_loanoffers_data_task >> [write_loanoffers_data_task] >> create_loanproducts_table_task >> [read_mongodb_loanproducts_data_task >> write_loanproducts_data_task] >> create_loanapplications_table_task >> [read_mongodb_loanapplications_data_task >> write_loanapplications_data_task] >> create_mlscore_table_task >> [read_mongodb_mlscore_data_task >> write_mlscore_data_task] >> create_loandeals_table_task >> [read_mongodb_loandeals_data_task >> write_loandeals_data_task]
    