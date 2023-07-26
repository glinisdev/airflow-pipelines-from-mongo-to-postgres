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
def read_mongodb_loanoffers_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["loanoffers"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                'financedAmount': 1,
                'period': 1,
                'minOffer': 1,
                'optOffer': 1
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        elem_dict["_id"] = element.get("_id", None)
        elem_dict["financedAmount"] = element.get("financedAmount", None)
        elem_dict["period"] = element.get("period", None)
        elem_dict["minOffer"] = element.get("minOffer", None)
        elem_dict["optOffer"] = element.get("optOffer", None)

        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/loanoffers_table.csv')


# Read from .csv and import to SQL function
def write_loanoffers_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/loanoffers_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.loanoffers (id, _id, financedAmount, period, minOffer, optOffer)" \
               "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO NOTHING"

        value = (index,  element["_id"],  element["financedAmount"],  element["period"], element["minOffer"], element["optOffer"])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_loanoffers_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/loanoffers.csv"

    if check_file_exists(bucket, filename, s3):

        s3.Bucket(bucket).download_file(filename, "loanoffers.csv")

        shutil.move('/opt/airflow/loanoffers.csv', '/opt/airflow/dags/data/daily_updates/loanoffers.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/loanoffers.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/loanoffers.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'loanoffers', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.loanoffers (id, _id, financedAmount, period, minOffer, optOffer)" \
                        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO NOTHING"

                    value = (index,  element["_id"],  element["financedAmount"],  element["period"], element["minOffer"], element["optOffer"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()

            except:
                IndexError, conn.close()
        
        else:
            pass
    else:
        pass