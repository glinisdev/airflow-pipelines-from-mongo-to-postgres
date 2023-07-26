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
def read_mongodb_users_data():

    load_dotenv()

    # Reading data from Mongo DB:
    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["users"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Personal information
                'username': 1,
                'personalInformation.firstName': 1,
                'personalInformation.lastName': 1,
                'personalInformation.email': 1,
                'personalInformation.phoneNumber': 1,

                # Business information
                'companyInformation.companyName': 1,
                'roles': 1,

                # Status
                'deleted': 1,
                'blocked': 1,
                'hasPassword': 1,
                'loggedIn': 1,
                'accountReviewed': 1,
                'validations': 1,

                # Dates
                'lastLogin': 1,
                'dateCreated': 1
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        # Personal information
        elem_dict["_id"] = element.get("_id", None)
        elem_dict["username"] = element.get("username", None)
        elem_dict["first_name"] = element.get("personalInformation", {}).get("firstName", None)
        elem_dict["last_name"] = element.get("personalInformation", {}).get("lastName", None)
        elem_dict["email"] = element.get("personalInformation", {}).get("email", None)
        elem_dict["phone_number"] = element.get("personalInformation", {}).get("phoneNumber", None)

        # Business information
        elem_dict["company_name"] = element.get("companyInformation", {}).get("companyName", None)
        elem_dict["roles"] = element.get("roles", None)

        # Status
        elem_dict["deleted"] = element.get("deleted", False)
        elem_dict["blocked"] = element.get("blocked", False)
        elem_dict["has_password"] = element.get("hasPassword", False)
        elem_dict["logged_in"] = element.get("loggedIn", False)
        elem_dict["account_reviewed"] = element.get("accountReviewed", False)
        elem_dict["validation_email"] = element.get("validations", {}).get("email", False)
        elem_dict["validation_phone_number"] = element.get( "validations", {}).get("phoneNumber", False)

        # Dates
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
        elem_dict["last_login"] = element.get("lastLogin", datetime(1990, 1, 1))

        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/users_table.csv')


# Read from .csv and import to SQL function
def write_users_date_to_SQL(schema_name):
    
    # Read .csv data
    df = pd.read_csv('dags/data/users_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.users (id, _id, username, first_name, last_name, email, phone_number, company_name, roles, deleted," \
            "blocked, has_password, logged_in, account_reviewed, validation_email, validation_phone_number, date_created, last_login)" \
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
            "SET roles = EXCLUDED.roles, deleted = EXCLUDED.deleted, blocked = EXCLUDED.blocked, has_password = EXCLUDED.has_password, logged_in = EXCLUDED.logged_in, "\
            "account_reviewed = EXCLUDED.account_reviewed, validation_email = EXCLUDED.validation_email, validation_phone_number = EXCLUDED.validation_phone_number, last_login = EXCLUDED.last_login"

        value = (index,  element["_id"],  element["username"],  element["first_name"], element["last_name"], element["email"], element["phone_number"],
                 element["company_name"], element["roles"], element["deleted"], element["blocked"], element["has_password"], element['logged_in'],
                 element["account_reviewed"], element["validation_email"], element["validation_phone_number"], element["date_created"], element["last_login"])

        try:
            cursor.execute(sql, value,)
            conn.commit()

        except BaseException:
            conn.commit()

    conn.close()


def write_users_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/users.csv"

    if check_file_exists(bucket, filename, s3):
        
        s3.Bucket(bucket).download_file(filename, "users.csv")

        shutil.move('/opt/airflow/users.csv', '/opt/airflow/dags/data/daily_updates/users.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/users.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/users.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id="postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'users', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.users (id, _id, username, first_name, last_name, email, phone_number, company_name, roles, deleted," \
                        "blocked, has_password, logged_in, account_reviewed, validation_email, validation_phone_number, date_created, last_login)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET roles = EXCLUDED.roles, deleted = EXCLUDED.deleted, blocked = EXCLUDED.blocked, has_password = EXCLUDED.has_password, logged_in = EXCLUDED.logged_in, "\
                        "account_reviewed = EXCLUDED.account_reviewed, validation_email = EXCLUDED.validation_email, validation_phone_number = EXCLUDED.validation_phone_number, last_login = EXCLUDED.last_login"

                    value = (index,  element["_id"],  element["username"],  element["first_name"], element["last_name"], element["email"], element["phone_number"],
                            element["company_name"], element["roles"], element["deleted"], element["blocked"], element["has_password"], element['logged_in'],
                            element["account_reviewed"], element["validation_email"], element["validation_phone_number"], element["date_created"], element["last_login"])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()

            except:
                IndexError, conn.close()

        else:
            pass
    else:
        pass
