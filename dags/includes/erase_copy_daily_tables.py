from datetime import datetime
from os import path, replace, remove
import shutil
import boto3


def erase_copy_daily_table(table_name):

   # Check for the file existence
    if path.isfile(f'dags/data/daily_updates/{table_name}.csv'):

        # Make a file copy
        shutil.copy(f'dags/data/daily_updates/{table_name}.csv', f'dags/data/daily_updates/{table_name}_copy.csv')

        # Create a backup copy in archieve folder
        file = f'dags/data/daily_updates/{table_name}.csv'
        location = f"dags/data/daily_archieve/{table_name}_{datetime.now().strftime('%d_%m_%Y')}.csv"
        replace(file, location)

        # Creating backup copy in archieve folder in S3
        s3 = boto3.resource('s3')
        bucket = 'avenews-airflow'
        filename = f"dags/data/daily_archieve/{table_name}_{datetime.now().strftime('%d_%m_%Y')}.csv"
        s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

        # Delete file
        remove(f'dags/data/daily_updates/{table_name}.csv')

    else:
        pass
