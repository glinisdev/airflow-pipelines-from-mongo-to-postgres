from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from dotenv import load_dotenv
from includes.utils import assign_new_pk_to_df, check_file_exists

import logging
import os
import pandas as pd
import pymongo
import shutil
import boto3


# Read from mongo and export .csv function

def read_mongodb_organizations_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["organizations"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Organization information
                'businessName': 1,
                'businessAddress': 1,
                'registrationNumber': 1,
                'type': 1,
                'valueChain': 1,

                # Users information
                'createdBy': 1,
                'orgUser': 1,
                'owner': 1,

                # Status
                'deleted': 1,

                # Dates
                'dateCreated': 1,

                # Onboarding information
                'onboardingInformation': 1
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        # Organization information
        elem_dict["_id"] = element.get("_id", None)
        elem_dict["business_name"] = element.get("businessName", None)
        elem_dict["registration_number"] = element.get("registrationNumber", None)      
        elem_dict["type"] = element.get("businessName", None)
        elem_dict["value_chain"] = element.get("valueChain", None)

        # User informarmation
        elem_dict["created_by"] = element.get("createdBy", None)
        elem_dict["org_user"] = element.get("orgUser", None)
        elem_dict["owner"] = element.get("owner", None)

        # Status
        elem_dict["deleted"] = element.get("deleted", False)

        # Dates
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

        # Onboarding information
        elem_dict["business_operations"] = element.get("onboardingInformation", {}).get("businessOperations", None)
        elem_dict["business_line"] = element.get("onboardingInformation", {}).get("businessLine", None)
        elem_dict["business_type"] = element.get("onboardingInformation", {}).get("businessType", None)
        elem_dict["business_date_created"] = element.get("onboardingInformation", {}).get("businessDateCreated", None)
        elem_dict["business_owner"] = element.get("onboardingInformation", {}).get("businessOwner", None)
        elem_dict["employees_amount"] = element.get("onboardingInformation", {}).get("employeesAmount", None)
        elem_dict["avenews_reason"] = element.get("onboardingInformation", {}).get("avenewsReason", None)
 
        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/organizations_table.csv')


# Read from .csv and import to SQL function
def write_organizations_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/organizations_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.organizations (id, _id, business_name, registration_number, type, value_chain, created_by, org_user, owner," \
               "deleted, date_created, business_operations, business_line, business_type, business_date_created, business_owner, employees_amount, avenews_reason)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE "\
               "SET deleted = EXCLUDED.deleted"

        value = (index,  element["_id"],  element["business_name"],  element["registration_number"], element["type"], element["value_chain"], 
                element["created_by"], element["org_user"], element["owner"], element["deleted"], element["date_created"], 
                element["business_operations"], element['business_line'], element["business_type"], element["business_date_created"], 
                element["business_owner"], element["employees_amount"], element["avenews_reason"])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_organizations_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/organizations.csv"

    if check_file_exists(bucket, filename, s3):

        s3.Bucket(bucket).download_file(filename, "organizations.csv")
        shutil.move('/opt/airflow/organizations.csv', '/opt/airflow/dags/data/daily_updates/organizations.csv')
        print("File downloaded on the path")

        if os.path.isfile('dags/data/daily_updates/organizations.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/organizations.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df   
            df.index = assign_new_pk_to_df(schema_name, 'organizations', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.organizations (id, _id, business_name, registration_number, type, value_chain, created_by, org_user, owner," \
                        "deleted, date_created, business_operations, business_line, business_type, business_date_created, business_owner, employees_amount, avenews_reason)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE "\
                        "SET deleted = EXCLUDED.deleted"

                    value = (index,  element["_id"],  element["business_name"],  element["registration_number"], element["type"], element["value_chain"], 
                            element["created_by"], element["org_user"], element["owner"], element["deleted"], element["date_created"], 
                            element["business_operations"], element['business_line'], element["business_type"], element["business_date_created"], 
                            element["business_owner"], element["employees_amount"], element["avenews_reason"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()
            except: IndexError, conn.close()

        else:
            pass
    else:
        logging.info('THERE IS NO FILE')
        pass
