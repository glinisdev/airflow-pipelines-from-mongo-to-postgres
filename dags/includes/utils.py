import boto3
from airflow.providers.postgres.hooks.postgres import PostgresHook


def assign_new_pk_to_df(schema_name, table_name, df):

    # Establish SQL connection:
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Read last PK
    cursor.execute(
        f"""
        SELECT id FROM {schema_name}.{table_name}
        ORDER BY id DESC
        LIMIT 1
        """
    )
    last_pk = cursor.fetchall()[0][0]

    # Check for PK existanse and assign PK to current df. If not - create new PK
    ids = list(df['_id'])
    p_keys = []
    i = 1

    for id in ids:

        sql = f"SELECT id FROM {schema_name}.{table_name} WHERE _id = '{id}'"
        cursor.execute(sql)

        try:
            key = cursor.fetchall()[0][0]
            p_keys.append(key)
        except:
            p_keys.append(last_pk+i)
            i += 1
            
    return p_keys


def check_file_exists(bucket, filename, s3):
    try:
        s3.Object(bucket, filename).load()
        return True
    except:
        return False
