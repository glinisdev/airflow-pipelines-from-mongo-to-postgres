from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

from includes.user_table import read_mongodb_users_data, write_users_date_to_SQL
from includes.organizations_table import read_mongodb_organizations_data, write_organizations_date_to_SQL
from includes.trades_table import read_mongodb_trades_data, write_trades_date_to_SQL
from includes.agribusiness_table import read_mongodb_agribusinesses_data, write_agribusinesses_date_to_SQL
from includes.invoices_table import read_mongodb_invoices_data, write_invoices_date_to_SQL
from includes.cashflow_events_table import read_mongodb_cashflow_events_data, write_cashfow_events_date_to_SQL
from includes.cashflow_events_goals_table import read_mongodb_cashflow_events_goals_data, write_cashfow_events_goals_date_to_SQL
from includes.accounts_table import read_mongodb_accounts_data, write_accounts_date_to_SQL


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
    dag_id="mongo_to_postgres_migration",
    default_args=default_dag_args,
    description="Create table => Read data from Mongo => Write data to postgres SQL",
    start_date=datetime(2023, 2, 7, 10),
    schedule_interval="@daily",
) as dag:

    # Create SQL tables
    create_users_table_task = PostgresOperator(
        task_id="create_users_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.users"
            "(id serial PRIMARY KEY, _id varchar UNIQUE, username varchar, first_name varchar, last_name varchar, email varchar,"
            "phone_number varchar, company_name varchar, roles varchar, deleted boolean, blocked boolean, has_password boolean,"
            "logged_in boolean, account_reviewed boolean, validation_email boolean, validation_phone_number boolean, date_created date, last_login date);"
    )

    create_organization_table_task = PostgresOperator(
        task_id="create_organizations_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.organizations"
             "(id serial PRIMARY KEY, _id varchar unique, business_name varchar, registration_number varchar, type varchar, value_chain varchar,"
             "created_by varchar, org_user varchar, owner varchar, deleted boolean, date_created date, business_operations varchar,"
             "business_line varchar, business_type varchar, business_date_created varchar, business_owner varchar, employees_amount varchar, avenews_reason varchar);"
    )

    create_trades_table = PostgresOperator(
        task_id="create_trades_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.trades"
        "(id SERIAL PRIMARY KEY, _id varchar unique, type varchar, name varchar, product_id varchar, product_name varchar, package_size decimal, measurement_unit varchar,"
            f"unit_price decimal, quantity decimal, total_price decimal, number varchar, organization varchar REFERENCES {schema_name}.organizations (_id), created_by varchar REFERENCES {schema_name}.users (_id),"
        "notes varchar, status varchar, deleted boolean, date date, due_date date, date_created date);"
    )

    create_agribusiness_table = PostgresOperator(
        task_id="create_agribusinesses_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.agribusinesses"
             "(id SERIAL PRIMARY KEY, _id varchar unique, organization varchar, business_details_name varchar, business_details_phone varchar,"
            f"referrers varchar, created_by varchar REFERENCES {schema_name}.users (_id), contact_deleted boolean, contact_first_name varchar, contact_last_name varchar, contact_id varchar,"
             "contact_date_created date, deleted boolean, date_created varchar);"
    )

    create_invoices_table = PostgresOperator(
        task_id="create_invoices_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.invoices"
             "(id SERIAL PRIMARY KEY, _id varchar unique, organization varchar, name varchar, phone_number varchar, email varchar, payment_terms decimal, payment_method varchar,"
            f"terms_and_conditions varchar, tax decimal, created_by varchar REFERENCES public.users (_id), product_id varchar, product_name varchar, product_package_size decimal,"
             "product_measurement_unit varchar, product_unit_price decimal, product_quantity decimal, deleted boolean, status varchar,"
             "issue_date date, supply_date date, due_date date, date_created date);"
    )

    create_cashflow_events_table = PostgresOperator(
        task_id="create_cashflow_events_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.cashflow_events"
            f"(id SERIAL PRIMARY KEY, _id varchar unique, organization varchar, amount decimal, type varchar, created_by varchar REFERENCES {schema_name}.users (_id),"
             "products varchar, deleted boolean, status varchar, date date, date_created date);"
    )

    create_cashflow_events_goals_table = PostgresOperator(
        task_id="create_cashflow_events_goal_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.cashflow_events_goals"
             "(id SERIAL PRIMARY KEY, _id varchar unique, organization varchar, total_amount decimal, month_amount decimal, goal varchar, way varchar,"
            f"notify varchar, created_by varchar REFERENCES {schema_name}.users (_id), deleted boolean, status varchar, date date, date_created date);"
    )

    create_accounts_table = PostgresOperator(
        task_id="create_accounts_table",
        postgres_conn_id=postgres_connection_name,
        sql=f"CREATE TABLE IF NOT EXISTS {schema_name}.accounts"
            f"(id SERIAL PRIMARY KEY, _id varchar unique, beneficiary_id varchar, details varchar, service varchar, created_by varchar,"
             "on_model varchar, deleted boolean, validated boolean, date_created date);"
    )

    # Read mongoDB data
    read_mongodb_users_data_task = PythonOperator(
        task_id="read_users_table",
        python_callable=read_mongodb_users_data
    )

    read_mongodb_organizations_data_task = PythonOperator(
        task_id="read_organizations_table",
        python_callable=read_mongodb_organizations_data

    )

    read_mongodb_trades_data_task = PythonOperator(
        task_id="read_trades_table",
        python_callable=read_mongodb_trades_data
    )

    read_mongodb_agribusinesses_data_task = PythonOperator(
        task_id="read_agribusinesses_table",
        python_callable=read_mongodb_agribusinesses_data
    )

    read_mongodb_invoices_data_task = PythonOperator(
        task_id="read_invoices_date",
        python_callable=read_mongodb_invoices_data
    )

    read_mongodb_cashflow_events_data_task = PythonOperator(
        task_id="read_cashflow_events_data",
        python_callable=read_mongodb_cashflow_events_data
    )

    read_mongodb_cashflow_events_goals_task = PythonOperator(
        task_id="read_cashflow_events_goals_data",
        python_callable=read_mongodb_cashflow_events_goals_data
    )

    read_mongodb_accounts_task = PythonOperator(
        task_id="read_accounts_data",
        python_callable=read_mongodb_accounts_data
    )

    # Write data to SQL
    write_users_to_SQL_task = PythonOperator(
        task_id="write_user_table",
        python_callable=write_users_date_to_SQL,
        op_args={schema_name}
    )

    write_organizations_to_SQL_task = PythonOperator(
        task_id="write_organizations_table",
        python_callable=write_organizations_date_to_SQL,
        op_args={schema_name}
    )

    write_trades_to_SQL_task = PythonOperator(
        task_id="write_trades_table",
        python_callable=write_trades_date_to_SQL,
        op_args={schema_name}
    )

    write_agribusinesses_to_SQL_task = PythonOperator(
        task_id="write_agribusinesses_table",
        python_callable=write_agribusinesses_date_to_SQL,
        op_args={schema_name}
    )

    write_invoices_to_SQL_task = PythonOperator(
        task_id="write_invoices_table",
        python_callable=write_invoices_date_to_SQL,
        op_args={schema_name}
    )

    write_cashflow_events_to_SQL_task = PythonOperator(
        task_id="write_cashflow_events_table",
        python_callable=write_cashfow_events_date_to_SQL,
        op_args={schema_name}
    )

    write_cashflow_events_goals_to_SQL_task = PythonOperator(
        task_id="write_cashflow_events_goals_table",
        python_callable=write_cashfow_events_goals_date_to_SQL,
        op_args={schema_name}
    )

    write_accounts_to_SQL_task = PythonOperator(
        task_id="write_accounts_table",
        python_callable=write_accounts_date_to_SQL,
        op_args={schema_name}
    )

    create_users_table_task >> read_mongodb_users_data_task >> [write_users_to_SQL_task] >> create_organization_table_task >> [read_mongodb_organizations_data_task >> write_organizations_to_SQL_task] >> create_trades_table >> [read_mongodb_trades_data_task >> write_trades_to_SQL_task] >> create_agribusiness_table >> [read_mongodb_agribusinesses_data_task >> write_agribusinesses_to_SQL_task] >> create_invoices_table >> [read_mongodb_invoices_data_task >> write_invoices_to_SQL_task] >> create_cashflow_events_table >> [read_mongodb_cashflow_events_data_task >> write_cashflow_events_to_SQL_task] >> create_cashflow_events_goals_table >> [read_mongodb_cashflow_events_goals_task >> write_cashflow_events_goals_to_SQL_task] >> create_accounts_table >> [read_mongodb_accounts_task >> write_accounts_to_SQL_task]