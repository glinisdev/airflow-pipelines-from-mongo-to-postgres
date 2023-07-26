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
def read_mongodb_mlscore_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["mlscoredatas"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {
                'loanId': 1,
                'score': 1,
                'categoriesTotalScore': 1,
                'dateCreated': 1
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        elem_dict["_id"] = element.get("_id", None)
        elem_dict["loanId"] = element.get("loanId", None)
        elem_dict["score"] = element.get("score", None)
        elem_dict["categoriesTotalScore"] = element.get("categoriesTotalScore", None)
        elem_dict["dateCreated"] = element.get("dateCreated", datetime(1990, 1, 1))

        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/mlscore_table.csv')


# Read from .csv and import to SQL function
def write_mlscore_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/mlscore_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.mlscore (id, _id, loanId, score, categoriesTotalScore, dateCreated)" \
               "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
               "SET score = EXCLUDED.score, categoriesTotalScore = EXCLUDED.categoriesTotalScore"

        value = (index, element["_id"], element["loanId"], element["score"], element["categoriesTotalScore"], element["dateCreated"])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_mlscore_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/mlscore.csv"

    if check_file_exists(bucket, filename, s3):
        s3.Bucket(bucket).download_file(filename, "mlscore.csv")

        shutil.move('/opt/airflow/mlscore.csv', '/opt/airflow/dags/data/daily_updates/mlscore.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/mlscore.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/mlscore.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'mlscore', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.mlscore (id, _id, loanId, score, categoriesTotalScore, dateCreated)" \
                        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET score = EXCLUDED.score, categoriesTotalScore = EXCLUDED.categoriesTotalScore"

                    value = (index, element["_id"], element["loanId"], element["score"], element["categoriesTotalScore"], element["dateCreated"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()
                
            except: 
                IndexError, conn.close()
        
        else:
            pass
    else:
        pass