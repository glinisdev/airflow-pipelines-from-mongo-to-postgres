from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone
from dotenv import load_dotenv
from includes.utils import assign_new_pk_to_df, check_file_exists

import os
import pandas as pd
import pymongo
import shutil
import boto3


# Read from mongo and export .csv function
def read_mongodb_loanapplications_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["loanapplications"]

    mongo_query = list(db_applications.aggregate([
    {
        '$match': {
            'dateCreated': {
                '$gt': datetime(2022, 10, 5, 0, 0, 0, tzinfo=timezone.utc)
            }
        }
    }, {
        '$unwind': {
            'path': '$products'
        }
    }, {
        '$project': {
            'personalDetails.email': 1,
            'personalDetails.primaryPhoneNumber': 1,
            'businessDetails.name': 1,
            'deleted': 1, 
            'dateCreated': 1, 
            'assignee': 1, 
            'status': 1, 
            'products': 1, 
            'dealId': 1
        }
    }
]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        elem_dict["_id"] = element.get("_id", None)
        elem_dict["deleted"] = element.get("deleted", False)
        elem_dict["dateCreated"] = element.get("dateCreated", datetime(1990, 1, 1))
        elem_dict['name'] = element.get("businessDetails", {}).get("name", None)
        elem_dict['email'] = element.get("personalDetails", {}).get("email", None)
        elem_dict['phoneNumber'] = element.get("personalDetails", {}).get("primaryPhoneNumber", None)
        elem_dict["status"] = element.get("status", None)
        elem_dict["assignee"] = element.get("assignee", None)
        elem_dict["products"] = element.get("products", None)
        elem_dict["dealId"] = element.get("dealId", None)

        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/loanapplications_table.csv')


# Read from .csv and import to SQL function
def write_loanapplications_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/loanapplications_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.loanapplications (id, _id, deleted, dateCreated, name, email, phoneNumber, status, assignee, products, dealId)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (products) DO UPDATE " \
                        "SET deleted = EXCLUDED.deleted, status = EXCLUDED.status"

        value = (index, element["_id"], element["deleted"], element["dateCreated"],  element["name"], element['email'], element['phoneNumber'], element["status"], element["assignee"],
                 element["products"], element["dealId"])
        try:
            cursor.execute(sql, value,)
            conn.commit()

        except BaseException:
            conn.commit()

    conn.close()


def write_loanapplications_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/loanapplications.csv"

    if check_file_exists(bucket, filename, s3):
        
        s3.Bucket(bucket).download_file(filename, "loanapplications.csv")

        shutil.move('/opt/airflow/loanapplications.csv', '/opt/airflow/dags/data/daily_updates/loanapplications.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/loanapplications.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/loanapplications.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'loanapplications', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.loanapplications (id, _id, deleted, dateCreated, name, email, phoneNumber, status, assignee, products, dealId)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (products) DO UPDATE " \
                        "SET deleted = EXCLUDED.deleted, status = EXCLUDED.status"

                    value = (index, element["_id"], element["deleted"], element["dateCreated"],  element["name"], element['email'], element['phoneNumber'], element["status"], element["assignee"],
                            element["products"], element["dealId"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()
            
            except:
                IndexError, conn.close()
                
        else:
            pass
    else:
        pass
