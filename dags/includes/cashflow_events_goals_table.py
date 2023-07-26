from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from dotenv import load_dotenv
from includes.utils import assign_new_pk_to_df, check_file_exists

import os
import pandas as pd
import pymongo
import shutil
import boto3


# Read from mongo and export .csv function
def read_mongodb_cashflow_events_goals_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["cashfloweventgoals"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Goal information
                'organization': 1,
                'totalAmount': 1,
                'monthAmount': 1,
                'goal': 1,
                'way': 1,
                'notify': 1,
                'createdBy': 1,              
                
                # Status
                'deleted': 1,
                'status': 1,

                # Dates
                'date': 1,
                'dateCreated': 1,
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        # Event information
        elem_dict["_id"] = element.get("_id", None)
        elem_dict["organization"] = element.get("organization", None)
        elem_dict["total_amount"] = element.get("totalAmount", None)
        elem_dict["month_amount"] = element.get("monthAmount", None)
        elem_dict["goal"] = element.get("goal", None)
        elem_dict["way"] = element.get("way", None)
        elem_dict["notify"] = element.get("notify", None)
        elem_dict["created_by"] = element.get("createdBy", None)

        # Status
        elem_dict["deleted"] = element.get("deleted", False)
        elem_dict["status"] = element.get("status", False)

        # Dates
        elem_dict["date"] = element.get("date", datetime(1990, 1, 1))
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
 
        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/cashflow_events_goals_table.csv')


# Read from .csv and import to SQL function
def write_cashfow_events_goals_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/cashflow_events_goals_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.cashflow_events_goals (id, _id, organization, total_amount, month_amount, goal, way, notify, created_by, deleted, status, date, date_created)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
               "SET deleted = EXCLUDED.deleted, status = EXCLUDED.status, date = EXCLUDED.date"

        value = (index,  element["_id"],  element["organization"],  element["total_amount"], element["month_amount"], element["goal"], element["way"], element["notify"],
                element["created_by"], element["deleted"], element["status"], element["date"], element["date_created"])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_cashfow_events_goals_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/cashflow_events_goals.csv"

    if check_file_exists(bucket, filename, s3):

        s3.Bucket(bucket).download_file(filename, "cashflow_events_goals.csv")

        shutil.move('/opt/airflow/cashflow_events_goals.csv', '/opt/airflow/dags/data/daily_updates/cashflow_events_goals.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/cashflow_events_goals.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/cashflow_events_goals.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'cashflow_events_goals', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.cashflow_events_goals (id, _id, organization, total_amount, month_amount, goal, way, notify, created_by, deleted, status, date, date_created)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET deleted = EXCLUDED.deleted, status = EXCLUDED.status, date = EXCLUDED.date"

                    value = (index,  element["_id"],  element["organization"],  element["total_amount"], element["month_amount"], element["goal"], element["way"], element["notify"],
                            element["created_by"], element["deleted"], element["status"], element["date"], element["date_created"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()

            except:
                IndexError, conn.close()
        
        else:
            pass
    else:
        pass
