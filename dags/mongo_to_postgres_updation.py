from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from includes.erase_copy_daily_tables import erase_copy_daily_table
from includes.user_table import write_users_date_to_SQL_daily
from includes.organizations_table import write_organizations_date_to_SQL_daily
from includes.trades_table import write_trades_date_to_SQL_daily
from includes.agribusiness_table import write_agribusinesses_date_to_SQL_daily
from includes.invoices_table import write_invoices_date_to_SQL_daily
from includes.cashflow_events_table import write_cashfow_events_date_to_SQL_daily
from includes.cashflow_events_goals_table import write_cashfow_events_goals_date_to_SQL_daily
from includes.accounts_table import write_accounts_date_to_SQL_daily


postgres_connection_name = "postgres_localhost" # Established connection with Airflow
schema_name = "public" # Desired schema name

# DAG settings

default_dag_args = {
    "owner": "George Linstein",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# Create DAG instanse

with DAG(
    dag_id="mongo_to_postgres_daily_update",
    default_args=default_dag_args,
    description="Read data from .csv => Write data to postgres SQL",
    start_date=datetime(2023, 2, 7, 10),
    schedule_interval="@daily",
) as dag:

    # Write data to SQL
    write_users_to_SQL_task = PythonOperator(
        task_id="write_user_table",
        python_callable=write_users_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_organizations_to_SQL_task = PythonOperator(
        task_id="write_organizations_table",
        python_callable=write_organizations_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_trades_to_SQL_task = PythonOperator(
        task_id="write_trades_table",
        python_callable=write_trades_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_agribusinesses_to_SQL_task = PythonOperator(
        task_id="write_agribusinesses_table",
        python_callable=write_agribusinesses_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_invoices_to_SQL_task = PythonOperator(
        task_id="write_invoices_table",
        python_callable=write_invoices_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_cashflow_events_to_SQL_task = PythonOperator(
        task_id="write_cashflow_events_table",
        python_callable=write_cashfow_events_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_cashflow_events_goals_to_SQL_task = PythonOperator(
        task_id="write_cashflow_events_goals_table",
        python_callable=write_cashfow_events_goals_date_to_SQL_daily,
        op_args={schema_name}
    )

    write_accounts_to_SQL_task = PythonOperator(
        task_id="write_accounts_table",
        python_callable=write_accounts_date_to_SQL_daily,
        op_args={schema_name}
    )

    # Erase csv file and create backup in archieve folder
    erase_copy_daily_users_table_task  = PythonOperator(
        task_id='erase_users_csv',
        python_callable=erase_copy_daily_table,
        op_args={'users'}
    )

    erase_copy_organizations_daily_table_task  = PythonOperator(
        task_id='erase_organizations_csv',
        python_callable=erase_copy_daily_table,
        op_args={'organizations'}
    )

    erase_copy_trades_daily_table_task  = PythonOperator(
        task_id='erase_trades_csv',
        python_callable=erase_copy_daily_table,
        op_args={'trades'}
    )
    
    erase_copy_agribusinesses_daily_table_task  = PythonOperator(
        task_id='erase_agribusinesses_csv',
        python_callable=erase_copy_daily_table,
        op_args={'agribusinesses'}
    )

    erase_copy_invoices_daily_table_task  = PythonOperator(
        task_id='erase_invoices_csv',
        python_callable=erase_copy_daily_table,
        op_args={'invoices'}
    )

    erase_copy_cashflow_events_daily_table_task  = PythonOperator(
        task_id='erase_cashflow_events_csv',
        python_callable=erase_copy_daily_table,
        op_args={'cashflow_events'}
    )

    erase_copy_cashflow_events_goals_table_task  = PythonOperator(
        task_id='erase_cashflow_events_goals_csv',
        python_callable=erase_copy_daily_table,
        op_args={'cashflow_events_goals'}
    )

    erase_copy_accounts_table_task  = PythonOperator(
        task_id='erase_accounts_csv',
        python_callable=erase_copy_daily_table,
        op_args={'accounts'}
    )


    [write_users_to_SQL_task, write_trades_to_SQL_task, write_agribusinesses_to_SQL_task, write_invoices_to_SQL_task, write_cashflow_events_to_SQL_task] >> write_cashflow_events_goals_to_SQL_task >> write_accounts_to_SQL_task >> [erase_copy_daily_users_table_task, erase_copy_organizations_daily_table_task, erase_copy_trades_daily_table_task, erase_copy_agribusinesses_daily_table_task, erase_copy_invoices_daily_table_task, erase_copy_cashflow_events_daily_table_task, erase_copy_cashflow_events_goals_table_task, erase_copy_accounts_table_task]