import psycopg2
import pandas as pd
import datetime
import json
import os
import boto3


# Setting up variables
print("Setting up environment variables")
AWS_ACCESS_KEY_ID = os.environ.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = os.environ.get('aws_secret_access_key')
AWS_SESSION_TOKEN = os.environ.get('aws_session_token')
AWS_REGION = os.environ.get('region')

S3_BUCKET = os.environ.get('S3_BUCKET_REPORT')

DB_HOST = os.environ.get('DB_HOST')
DB_DATABASE = os.environ.get('DB_DATABASE')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_PORT = str(os.environ.get('DB_PORT'))


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
            )

        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def query_execute(title, query):
    """ Execution of sql statements and output the results"""
    conn = connect()

    # create a cursor
    cur = conn.cursor()

    # execute a statement
    sql=open('/report/sql/' + query, "r").read()
    cur.execute(sql)
    
    df = pd.DataFrame(cur.fetchall())
    colnames = [desc[0] for desc in cur.description]
    df.columns = colnames

    f.write('\n' + title + '\n') 
    f.write("=" * 70 + '\n')
    f.write(df.to_string(index=False))
    f.write('\n\n')
    
    if conn is not None:
        conn.close()


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3',
        region_name = AWS_REGION,
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        aws_session_token = AWS_SESSION_TOKEN
        )

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


if __name__ == '__main__':

    # Opening JSON file
    with open('queries.json') as json_file:
        queries = json.load(json_file)
  
    # Save report to file
    report_file_name = "report_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
    report_full_path = f"/report/output/{report_file_name}"

    with open(report_full_path, 'w') as f:
        
        f.write('\n')
        f.write('========================================================================================= \n')
        f.write('COVID-19. Casos registrados en la República Argentina año 2022 \n')
        f.write('Fuente: https://datos.gob.ar/dataset/salud-covid-19-casos-registrados-republica-argentina \n')
        f.write('========================================================================================= \n')

        for query in queries.values():
            print(f"Running query {query['query']}")
            query_execute(query['title'], query['query'])

    
    # Create an S3 access object
    s3 = boto3.client("s3")

    local_file = report_full_path
    s3_file_name = report_file_name

    uploaded = upload_to_aws(local_file, S3_BUCKET, s3_file_name)
    
   