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
def read_mongodb_trades_data():

    # Get method for list
    def safe_list_get(l, idx, default):
        try:
            return l[idx]
        except IndexError:
            return default
   
    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["trades"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Trade information
                'type': 1,
                'name': 1,
                'products': 1,
                'totalPrice': 1,
                'number': 1,
                'organization': 1,
                'createdBy': 1,
                'notes': 1,

                # Status
                'status': 1,
                'deleted': 1,

                # Dates
                'date': 1,
                'dueDate': 1,
                'dateCreated': 1
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        # Trade information
        elem_dict["_id"] = element.get("_id", None)
        elem_dict["type"] = element.get("type", None)
        elem_dict["name"] = element.get("name", None)

        elem_dict["product_id"] = safe_list_get(element["products"], 0, {}).get("productId", None)
        elem_dict["product_name"] = safe_list_get(element["products"], 0, {}).get("name", None)
        elem_dict["package_size"] = safe_list_get(element["products"], 0, {}).get("packageSize", None)
        elem_dict["measurement_unit"] = safe_list_get(element["products"], 0, {}).get("measurementUnit", None)
        elem_dict["unit_price"] = safe_list_get(element["products"], 0, {}).get("unitPrice", None)
        elem_dict["quantity"] = safe_list_get(element["products"], 0, {}).get("quantity", None)

        elem_dict["total_price"] = element.get("totalPrice", None)
        elem_dict["number"] = element.get("number", None)
        elem_dict["organization"] = element.get("organization", None)
        elem_dict["created_by"] = element.get("createdBy", None)
        elem_dict["notes"] = element.get("notes", None)

        # Status
        elem_dict["status"] = element.get("status", None)      
        elem_dict["deleted"] = element.get("deleted", False)

        # Dates
        elem_dict["date"] = element.get("date", datetime(1990, 1, 1))
        elem_dict["due_date"] = element.get("dueDate", datetime(1990, 1, 1))
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
 
        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/trades_table.csv')


# Read from .csv and import to SQL function
def write_trades_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/trades_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.trades (id, _id, type, name, product_id, product_name, package_size, measurement_unit, unit_price, quantity," \
               "total_price, number, organization, created_by, notes, status, deleted, date, due_date, date_created)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
               "SET date = EXCLUDED.date, status = EXCLUDED.status, notes = EXCLUDED.notes, deleted = EXCLUDED.deleted"

        value = (index,  element["_id"],  element["type"],  element["name"], element["product_id"], element["product_name"], element["package_size"], 
                element["measurement_unit"], element["unit_price"], element["quantity"], element["total_price"], element["number"], element["organization"], 
                element["created_by"], element["notes"], element["status"], element["deleted"], element["date"], element["due_date"], element['date_created'])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_trades_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/trades.csv"

    if check_file_exists(bucket, filename, s3):

        s3.Bucket(bucket).download_file(filename, "trades.csv")

        shutil.move('/opt/airflow/trades.csv', '/opt/airflow/dags/data/daily_updates/trades.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/trades.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/trades.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'trades', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.trades (id, _id, type, name, product_id, product_name, package_size, measurement_unit, unit_price, quantity," \
                        "total_price, number, organization, created_by, notes, status, deleted, date, due_date, date_created)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET date = EXCLUDED.date, status = EXCLUDED.status, notes = EXCLUDED.notes, deleted = EXCLUDED.deleted"

                    value = (index,  element["_id"],  element["type"],  element["name"], element["product_id"], element["product_name"], element["package_size"], 
                            element["measurement_unit"], element["unit_price"], element["quantity"], element["total_price"], element["number"], element["organization"], 
                            element["created_by"], element["notes"], element["status"], element["deleted"], element["date"], element["due_date"], element['date_created'])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()

            except:
                IndexError, conn.close()

        else:
            pass
    else:
        pass