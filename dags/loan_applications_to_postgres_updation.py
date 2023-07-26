from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from includes.erase_copy_daily_tables import erase_copy_daily_table
from includes.loanoffers_table import write_loanoffers_date_to_SQL_daily
from includes.loanapplication_table import write_loanapplications_date_to_SQL_daily
from includes.loanproducts_table import write_loanproducts_date_to_SQL_daily
from includes.mlscore_table import write_mlscore_date_to_SQL_daily
from includes.loandeals_table import write_loandeals_date_to_SQL_daily

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
    dag_id="loan_applications_to_postgres_daily_update",
    default_args=default_dag_args,
    description="Read data from .csv => Write data to postgres SQL",
    start_date=datetime(2023, 2, 7, 10),
    schedule_interval="00 02 * * *",
) as dag:
  
    # Write data to SQL
    write_loanoffers_data_task = PythonOperator(
        task_id="write_loanoffers_table",
        python_callable=write_loanoffers_date_to_SQL_daily,
        op_args={schema_name}
    )
    write_loanproducts_data_task = PythonOperator(
        task_id="write_loanproducts_table",
        python_callable=write_loanproducts_date_to_SQL_daily,
        op_args={schema_name}
    )
    write_loanapplications_data_task = PythonOperator(
        task_id="write_loanapplications_table",
        python_callable=write_loanapplications_date_to_SQL_daily,
        op_args={schema_name}
    )
    write_mlscore_data_task = PythonOperator(
        task_id="write_mlscore_table",
        python_callable=write_mlscore_date_to_SQL_daily,
        op_args={schema_name}
    )
    write_loandeals_data_task = PythonOperator(
        task_id="write_loandeals_table",
        python_callable=write_loandeals_date_to_SQL_daily,
        op_args={schema_name}
    )

    # Erase csv file and create backup in archieve folder
    erase_copy_daily_loanoffers_table_task  = PythonOperator(
        task_id='erase_loanoffers_csv',
        python_callable=erase_copy_daily_table,
        op_args={'loanoffers'}
    )
    
    erase_copy_daily_loanproducts_table_task  = PythonOperator(
        task_id='erase_loanproducts_csv',
        python_callable=erase_copy_daily_table,
        op_args={'loanproducts'}
    )

    erase_copy_daily_loanapplications_table_task  = PythonOperator(
        task_id='erase_loanapplications_csv',
        python_callable=erase_copy_daily_table,
        op_args={'loanapplications'}
    )

    erase_copy_daily_mlscore_table_task  = PythonOperator(
        task_id='erase_mlscore_csv',
        python_callable=erase_copy_daily_table,
        op_args={'mlscore'}
    )
        
    erase_copy_daily_loandeals_table_task  = PythonOperator(
        task_id='erase_loandeals_csv',
        python_callable=erase_copy_daily_table,
        op_args={'loandeals'}
    )


    [write_loanoffers_data_task, write_loanapplications_data_task, write_loandeals_data_task] >> write_loanproducts_data_task >> write_mlscore_data_task >> [erase_copy_daily_loanapplications_table_task, erase_copy_daily_loandeals_table_task, erase_copy_daily_loanoffers_table_task, erase_copy_daily_loanproducts_table_task, erase_copy_daily_mlscore_table_task]