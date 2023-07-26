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
def read_mongodb_agribusinesses_data():

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
    db_applications = mongo_db["agribusinesses"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Agribusiness information
                'organization': 1,
                'businessDetails': 1,
                'referrers': 1,
                'contacts': 1,
                'createdBy': 1,

                # Status
                'deleted': 1,

                # Dates
                'dateCreated': 1
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        # Agribusiness information
        elem_dict["_id"] = element.get("_id", None)
        elem_dict["organization"] = element.get("organization", None)
        elem_dict["business_details_name"] = element.get("businessDetails", {}).get("name", None)
        elem_dict["business_details_phone"] = element.get("businessDetails", {}).get("phoneNumber", None)
        elem_dict["referrers"] = safe_list_get(str(element["referrers"]), 0, None)
        elem_dict["created_by"] = element.get("createdBy", None)

        # Contact information
        if element['contacts']:
            elem_dict["contact_deleted"] = safe_list_get(element["contacts"], 0, {}).get("deleted", False)
            elem_dict["contact_first_name"] = safe_list_get(element["contacts"], 0, {}).get("firstName", None)
            elem_dict["contact_last_name"] = safe_list_get(element["contacts"], 0, {}).get("lastName", None)
            elem_dict["contact_id"] = safe_list_get(element["contacts"], 0, {}).get("_id")
            elem_dict["contact_date_created"] = safe_list_get(element["contacts"], 0, {}).get("dateCreated", datetime(1990, 1, 1))
        else:
            elem_dict["contact_deleted"] = False
            elem_dict["contact_first_name"] = None
            elem_dict["contact_last_name"] = None
            elem_dict["contact_id"] = None
            elem_dict["contact_date_created"] = datetime(1990, 1, 1)

        # Status
        elem_dict["deleted"] = element.get("deleted", False)

        # Dates
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
 
        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/agribusinesses_table.csv')


# Read from .csv and import to SQL function
def write_agribusinesses_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/agribusinesses_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.agribusinesses (id, _id, organization, business_details_name, business_details_phone, referrers, created_by, contact_deleted, contact_first_name," \
               "contact_last_name, contact_id, contact_date_created, deleted, date_created)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
               "SET business_details_name = EXCLUDED.business_details_name, contact_deleted = EXCLUDED.contact_deleted, deleted = EXCLUDED.deleted"

        value = (index,  element["_id"],  element["organization"],  element["business_details_name"], element["business_details_phone"], 
                element["referrers"], element["created_by"], element["contact_deleted"], element["contact_first_name"], element["contact_last_name"], 
                element["contact_id"], element["contact_date_created"], element["deleted"], element['date_created'])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_agribusinesses_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/agribusinesses.csv"

    if check_file_exists(bucket, filename, s3):

        s3.Bucket(bucket).download_file(filename, "agribusinesses.csv")

        shutil.move('/opt/airflow/agribusinesses.csv', '/opt/airflow/dags/data/daily_updates/agribusinesses.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/agribusinesses.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/agribusinesses.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'agribusinesses', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.agribusinesses (id, _id, organization, business_details_name, business_details_phone, referrers, created_by, contact_deleted, contact_first_name," \
                        "contact_last_name, contact_id, contact_date_created, deleted, date_created)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET business_details_name = EXCLUDED.business_details_name, contact_deleted = EXCLUDED.contact_deleted, deleted = EXCLUDED.deleted"

                    value = (index,  element["_id"],  element["organization"],  element["business_details_name"], element["business_details_phone"], 
                            element["referrers"], element["created_by"], element["contact_deleted"], element["contact_first_name"], element["contact_last_name"], 
                            element["contact_id"], element["contact_date_created"], element["deleted"], element['date_created'])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()

            except:
                IndexError, conn.close()
        
        else:
            pass
    else:
        pass