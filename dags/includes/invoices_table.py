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
def read_mongodb_invoices_data():

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
    db_applications = mongo_db["invoices"]

    mongo_query = list(db_applications.aggregate([
        {
            '$project': {

                # Invoice information
                'organization': 1,
                'name': 1,
                'address': 1,
                'phoneNumber': 1,
                'email': 1,
                'paymentTerms': 1,
                'paymentMethod': 1,
                'termsAndConditions': 1,
                'taxPercentaje': 1,
                'total': 1,
                'createdBy': 1,
                
                # Product information               
                'products': 1,

                # Status
                'deleted': 1,
                'status': 1,

                # Dates
                'issueDate': 1,
                'supplyDate': 1,
                'dueDate': 1,
                'dateCreated': 1,
            }
        }
    ]))

    # Transforming data
    elements_array = []

    for element in mongo_query:
        elem_dict = {}

        # Invoice information
        elem_dict["_id"] = element.get("_id", None)
        elem_dict["organization"] = element.get("organization", None)
        elem_dict["name"] = element.get("name", None)
        elem_dict["phone_number"] = element.get("phoneNumber", None)
        elem_dict["email"] = element.get("email", None)
        elem_dict["payment_method"] = element.get("paymentMethod", None)
        elem_dict["payment_terms"] = element.get("paymentTerms", None)
        elem_dict["terms_and_conditions"] = element.get("termsAndConditions", None)
        elem_dict["tax"] = element.get("taxPercentaje", None)
        elem_dict["created_by"] = element.get("createdBy", None)

        # Product information
        if element.get("products"):
            elem_dict["product_id"] = safe_list_get(element["products"], 0, {}).get("productId", None)
            elem_dict["product_name"] = safe_list_get(element["products"], 0, {}).get("name", None)
            elem_dict["product_package_size"] = safe_list_get(element["products"], 0, {}).get("packageSize", None)
            elem_dict["product_measurement_unit"] = safe_list_get(element["products"], 0, {}).get("measurementUnit", None)
            elem_dict["product_unit_price"] = safe_list_get(element["products"], 0, {}).get("unitPrice", None)
            elem_dict["product_quantity"] = safe_list_get(element["products"], 0, {}).get("quantity", None)
        else:
            elem_dict["product_id"] = None
            elem_dict["product_name"] = None
            elem_dict["product_package_size"] = None
            elem_dict["product_measurement_unit"] = None
            elem_dict["product_unit_price"] = None
            elem_dict["product_quantity"] = None

        # Status
        elem_dict["deleted"] = element.get("deleted", False)
        elem_dict["status"] = element.get("status", False)

        # Dates
        elem_dict["issue_date"] = element.get("issueDate", datetime(1990, 1, 1))
        elem_dict["supply_date"] = element.get("supplyDate", datetime(1990, 1, 1))
        elem_dict["due_date"] = element.get("dueDate", datetime(1990, 1, 1))
        elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
 
        # Append to array
        elements_array.append(elem_dict)

    # Write to .csv file
    pd.DataFrame(elements_array).to_csv('dags/data/invoices_table.csv')

# Read from .csv and import to SQL function
def write_invoices_date_to_SQL(schema_name):

    # Read .csv data
    df = pd.read_csv('dags/data/invoices_table.csv', index_col=0)

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Writing data to Postgres DB:
    for index, element in df.iterrows():
        sql = f"INSERT INTO {schema_name}.invoices (id, _id, organization, name, phone_number, email, payment_terms, payment_method, terms_and_conditions," \
               "tax, created_by, product_id, product_name, product_package_size, product_measurement_unit, product_unit_price, product_quantity," \
               "deleted, status, issue_date, supply_date, due_date, date_created)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
               "SET deleted = EXCLUDED.deleted, status = EXCLUDED.status"

        value = (index,  element["_id"],  element["organization"],  element["name"], element["phone_number"], element["email"], element["payment_terms"],
                element["payment_method"], element["terms_and_conditions"], element["tax"], element["created_by"], element["product_id"], element["product_name"], 
                element["product_package_size"], element["product_measurement_unit"], element["product_unit_price"], element["product_quantity"], element["deleted"], 
                element["status"], element["issue_date"], element["supply_date"], element["due_date"], element['date_created'])

        cursor.execute(sql, value,)
        conn.commit()

    conn.close()


def write_invoices_date_to_SQL_daily(schema_name):
    s3 = boto3.resource('s3')
    bucket = "avenews-airflow"
    filename = "dags/data/daily_updates/invoices.csv"

    if check_file_exists(bucket, filename, s3):
        
        s3.Bucket(bucket).download_file(filename, "invoices.csv")

        shutil.move('/opt/airflow/invoices.csv', '/opt/airflow/dags/data/daily_updates/invoices.csv')
        print("File downloaded on the path")

        # Check for file existence
        if os.path.isfile('dags/data/daily_updates/invoices.csv'):

            # Read .csv data
            df = pd.read_csv('dags/data/daily_updates/invoices.csv', index_col=0)
            df.sort_values(['_id'], ascending=True, inplace=True)

            # Establish SQL connection:
            hook = PostgresHook(postgres_conn_id = "postgres_localhost")
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Assing old PK and new PK to df       
            df.index = assign_new_pk_to_df(schema_name, 'invoices', df)

            # Writing data to Postgres DB:
            try:
                for index, element in df.iterrows():
                    sql = f"INSERT INTO {schema_name}.invoices (id, _id, organization, name, phone_number, email, payment_terms, payment_method, terms_and_conditions," \
                        "tax, created_by, product_id, product_name, product_package_size, product_measurement_unit, product_unit_price, product_quantity," \
                        "deleted, status, issue_date, supply_date, due_date, date_created)" \
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (_id) DO UPDATE " \
                        "SET deleted = EXCLUDED.deleted, status = EXCLUDED.status"

                    value = (index,  element["_id"],  element["organization"],  element["name"], element["phone_number"], element["email"], element["payment_terms"],
                            element["payment_method"], element["terms_and_conditions"], element["tax"], element["created_by"], element["product_id"], element["product_name"], 
                            element["product_package_size"], element["product_measurement_unit"], element["product_unit_price"], element["product_quantity"], element["deleted"], 
                            element["status"], element["issue_date"], element["supply_date"], element["due_date"], element['date_created'])

                    cursor.execute(sql, value,)
                    conn.commit()

                conn.close()
            except:
                IndexError, conn.close()

        else:
            pass
    else:
        pass