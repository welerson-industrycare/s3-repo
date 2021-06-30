import os
import psycopg2
import boto3
import csv
from datetime import datetime
from config import config
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv

time = datetime.now()
now = time.strftime('%Y-%m-%d %H:%M:%S')

load_dotenv()  # take environment variables from .env.

ACCESS_KEY=os.getenv("ACCESS_KEY")
SECRET_KEY=os.getenv("SECRET_KEY")


s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


def proccess():

    try:

        company = get_company()

        date = now.split('-')[0] +'-'+ now.split('-')[1]

        create_csv()

        response = upload_object(f'../files/utility_{date}.csv', f'{company}-bucket', f'Dados {company.upper()}/utility_{date}.csv')


        if response:
            delete_data()

    except Exception as error:
        print(error)


def get_company():

    conn = None

    sql = """
        SELECT
        report_db
        FROM company
    """

    try:
        #Connection to postgres
        conn = connect()

        cur = conn.cursor()

        cur.execute(sql)

        data = cur.fetchone()

        company_name = data[0]

        cur.close()

    except Exception as error:
        print(f'Horário: {now}')
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print(" PostgreSQL connection is closed\n")
            return company_name



def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

    except Exception as error:
        print(error)
        
    finally:
        if conn is not None:
            return conn
            print('Database connection closed.')



def retrieve_data():

    conn = None

    sql = f"""
        SELECT
        utility_id AS utility_id, 
        TO_CHAR(datetime_read, 'YYYY-MM-DD HH24:MI') AS datetime_read,
        TO_CHAR(datetime_register, 'YYYY-MM-DD HH24:MI') AS datetime_register,
        value::FLOAT AS value,
        plant_equipment_id AS plant_equipment_id 
        FROM
        utility
        WHERE datetime_read <= '{now}'
        ORDER BY 1 
    """

    try:
        #Connection to postgres
        conn = connect()

        cur = conn.cursor()

        data = []

        cur.execute(sql)

        data = cur.fetchall()

        cur.close()

    except Exception as error:
        print(f'Horário: {now}')
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print(f'\n Horário: {now}\n')
            print(f' Total de registros coletados da utility: {len(data)} \n')
            print(" PostgreSQL connection is closed\n")
            return data



def delete_data():

    conn = None

    sql = f"""
        DELETE FROM utility
        WHERE datetime_read <= '{now}' 
    """

    try:
        #Connection to postgres
        conn = connect()

        cur = conn.cursor()

        cur.execute(sql)

        deleted_data = cur.rowcount 

        conn.commit()

        cur.close()

    except Exception as error:
        print(f'Horário: {now}')
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print(f'\n Horário: {now}\n')
            print(f' Total de registros deletados da utility: {deleted_data} \n')
            print(" PostgreSQL connection is closed\n")



def create_csv():

    data = retrieve_data()

    date = now.split('-')[0] +'-'+ now.split('-')[1]

    with open(f'../files/utility_{date}.csv', mode="w") as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['utility_id','datetime_register','datetime_read', 'value', 'plant_equipment_id'])
        writer.writerows(data)




def upload_object(file_name, bucket, object_name=None):


    if object_name is None:
        object_name = file_name
    
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



def list_objects_bucket(bucket_name):
    response = s3.list_objects(Bucket=bucket_name)

    print(f'Resposta: {response}\n')
    for objects in response['Contents']:
        print(objects['Key'])

if __name__ == '__main__':
    proccess()
