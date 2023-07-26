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
def read_mongodb_accounts_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["accounts"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Account information
                'beneficiaryId': 1,
                'details': 1,
                'service': 1,
                'createdBy': 1,
                'onModel': 1,
                
                # Status
                'deleted': 1,
                'validated': 1,

                # Dates
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
        elem_dict["beneficiary_id"] = element.get("beneficiaryId", None)
        elem_dict["details"] = element.get("details", None)
        elem_dict["service"] = element.get("service", None)
        elem_dict["created_by"] = element.get("createdBy", None)
        elem_dict["on_model"] = element.get("onModel", None)
        
        # Status
        elem_dict["deleted"] = element.get("deleted", False)
        elem_dict["validated"] = element.get("validated", False)

        # Dates
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
 
        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/accounts_table.csv')


# Read from .csv and import to SQL function
def write_accounts_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/accounts_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.accounts (id, _id, beneficiary_id, details, service, created_by, on_model, deleted, validated, date_created)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
               "SET details = EXCLUDED.details , service = EXCLUDED.service," \
               "on_model = EXCLUDED.on_model, deleted = EXCLUDED.deleted, validated = EXCLUDED.validated"

        value = (index,  element["_id"],  element["beneficiary_id"],  element["details"], element["service"], element["created_by"],
                element["on_model"], element["deleted"], element["validated"], element["date_created"])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_accounts_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/accounts.csv"

    if check_file_exists(bucket, filename, s3):

        s3.Bucket(bucket).download_file(filename, "accounts.csv")

        shutil.move('/opt/airflow/accounts.csv', '/opt/airflow/dags/data/daily_updates/accounts.csv')
        print("File downloaded on the path")


        if os.path.isfile('dags/data/daily_updates/accounts.csv'):
            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/accounts.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # Assing old PK and new PK to df   
            df.index = assign_new_pk_to_df(schema_name, 'accounts', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.accounts (id, _id, beneficiary_id, details, service, created_by, on_model, deleted, validated, date_created)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET details = EXCLUDED.details , service = EXCLUDED.service," \
                        "on_model = EXCLUDED.on_model, deleted = EXCLUDED.deleted, validated = EXCLUDED.validated"

                    value = (index,  element["_id"],  element["beneficiary_id"],  element["details"], element["service"], element["created_by"],
                            element["on_model"], element["deleted"], element["validated"], element["date_created"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()

            except: IndexError, conn.close()

        else:
            pass
    else:
        pass