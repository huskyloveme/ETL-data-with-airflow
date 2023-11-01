import os
import sys
import pendulum
from bs4 import BeautifulSoup
import subprocess
import pandas as pd
import subprocess
from list_atri import list_field
# from schema_data import schema, schema_sql
import json
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery, storage

dags_dir = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(dags_dir, os.pardir))
connections_dir = os.path.join(parent_directory, 'connections')
sys.path.append(connections_dir)

from mysql_con import cursor, connection
from mongodb import collection, client

# AUTHEN GGCLOUD
file_path_key = os.path.join(dags_dir, 'key_authorization.json')

# Authenticate and create a BigQuery client
client_gg = bigquery.Client.from_service_account_json(file_path_key)

# Authenticate and create a GCS client
storage_client = storage.Client.from_service_account_json(file_path_key)


#=================EXTRACT===================

def extract_data_mongo_to_cgs(records):
    def remove_html_tags(html_string):
        soup = BeautifulSoup(html_string, "html.parser")
        plain_text = soup.get_text()
        return plain_text
    # path file
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'data_mongo.jsonl')

    # clear file
    with open(file_path, 'w') as file:
        file.write('')

    tmp = 0
    count_mor = 0
    while True:
        result = collection.find({}).skip(tmp).limit(10000)
        stop_w = 0
        for k, i in enumerate(list(result)):
            count_mor += 1
            stop_w += k
            # print(count_mor)
            copy_dict = i.copy()
            skip_record = False
            with open(file_path, 'a') as file:
                for key, value in copy_dict.items():
                    # Handle origin
                    if key == 'specifications':
                        origin = "None"
                        for j in value:
                            if j.get('name') == 'Content':
                                for x in j['attributes']:
                                    if x.get('code') == 'origin':
                                        origin = x.get('value')
                        i["origin"] = origin

                    # Handle html
                    if key == 'description':
                        i["description"] = remove_html_tags(value)
                    # Skip outstock
                    if key == 'inventory_status' and value != 'available':
                        skip_record = True

                    if key not in list_field:
                        del i[key]
                if skip_record:
                    continue
                json_line = json.dumps(i, default=str)
                file.write(json_line + '\n')
            if count_mor == records:
                break
        if count_mor == records:
            break
        if stop_w == 0:
            break
        tmp += 10000
    client.close()
    # Push data to GCS
    # ========================================================================
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path_log = os.path.join(current_directory, 'log.txt')
    try:
        bucket = storage_client.bucket('upload_file_minh')
        blob = bucket.blob('data_lake_airflow/data_mongo.jsonl')
        blob.upload_from_filename(file_path)

        with open(file_path_log, 'a') as file:
            file.write("Lệnh đã được thực thi thành công. - ")
            file.write(str(datetime.now().strftime('%A, %d %B %Y %H:%M:%S')) + "\n")
        print("DONE")
        return True
    except Exception as e:
        with open(file_path_log, 'a') as file:
            file.write("Lệnh thất bại với mã lỗi: + " + str(e) + " - ")
            file.write(str(datetime.now().strftime('%A, %d %B %Y %H:%M:%S')) + "\n")
        print("FAILED")
        return False

def extract_data_mysql_to_cgs():
    query = 'SELECT * from final_data'
    cursor.execute(query)
    result_data_col = cursor.fetchall()
    header_data = [column[0] for column in cursor.description]
    # path file
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'data_mysql.jsonl')

    # clear file
    with open(file_path, 'w') as file:
        file.write('')

    for ind, record in enumerate(result_data_col):
        print(ind)
        with open(file_path, 'a') as file:
            dict_record = {
                'itemID': record[0],
                'title': record[1],
                'branding': record[2],
                'rating': record[3],
                'count_rating': record[4],
                'current_price': record[5],
                'shipping': record[6],
                'image_url': record[7],
                'max_resolution': record[8],
                'displayport': record[9],
                'hdmi': record[10],
                'directx': record[11],
                'model': record[12]
            }
            json_line = json.dumps(dict_record, default=str)
            file.write(json_line + '\n')

    # Push data to GCS
    # ========================================================================
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path_log = os.path.join(current_directory, 'log.txt')

    try:
        bucket = storage_client.bucket('upload_file_minh')
        blob = bucket.blob('data_lake_airflow/data_mysql.jsonl')
        blob.upload_from_filename(file_path)
        with open(file_path_log, 'a') as file:
            file.write("Lệnh đã được thực thi thành công. - ")
            file.write(str(datetime.now().strftime('%A, %d %B %Y %H:%M:%S')) + "\n")
        print("DONE")
    except Exception as e:
        with open(file_path_log, 'a') as file:
            file.write("Lệnh thất bại với mã lỗi: + " + str(e) + " - ")
            file.write(str(datetime.now().strftime('%A, %d %B %Y %H:%M:%S')) + "\n")
        print("FAILED")


#=================TRANSFROM=================

def transfrom_data_mysql_to_bigquery():
    check_mysql = True
    # Defind
    load_job_config = bigquery.LoadJobConfig()
    bigquery_dataset = client_gg.get_dataset('airflow')

    # PROCESS MYSQL
    try:
        load_job_config.write_disposition = 'WRITE_TRUNCATE'
        load_job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        load_job_config.autodetect = "TRUE"
        uri = "gs://upload_file_minh/data_lake_airflow/data_mysql.jsonl"
        bucket = storage_client.bucket('upload_file_minh')
        blob = bucket.blob('data_lake_airflow/data_mysql.jsonl')
        file_exists = blob.exists()
        if file_exists:
            # Tranform
            job = client_gg.load_table_from_uri(uri, bigquery_dataset.table('mysql_data'), job_config=load_job_config)
            job.result()
        else:
            check_mysql = False
    except Exception as err:
        print(str(err))
        check_mysql = False

    return check_mysql

def transfrom_data_mongo_to_bigquery():
    check_mongodb = True
    # Defind
    load_job_config = bigquery.LoadJobConfig()
    bigquery_dataset = client_gg.get_dataset('airflow')

    # PROCESS MONGODB
    try:
        load_job_config.write_disposition = 'WRITE_TRUNCATE'
        load_job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        load_job_config.schema = schema
        uri = "gs://upload_file_minh/data_lake_airflow/data_mongo.jsonl"
        bucket = storage_client.bucket('upload_file_minh')
        blob = bucket.blob('data_lake_airflow/data_mongo.jsonl')
        file_exists = blob.exists()
        if file_exists:
            # Tranform
            job = client_gg.load_table_from_uri(uri, bigquery_dataset.table('mongodb_data'), job_config=load_job_config)
            job.result()
        else:
            check_mongodb = False
    except Exception as err:
        print(str(err))
        check_mongodb = False

    return check_mongodb

#==================LOAD=====================
def load_data_mongo_to_datamart():

    view_id = 'factory-hackathon-2023.airflow.mongo_db_query_1'
    view = bigquery.Table(view_id)
    # TOP 10 BAN CHAY
    view.view_query = "SELECT t.name as name, count(1) as COUNT FROM `factory-hackathon-2023.airflow.mongodb_data` as a, UNNEST (categories) as t\
     WHERE t.name != 'Root' \
     GROUP BY  t.name \
     ORDER BY count(1) DESC \
     LIMIT 30 "

    try:
        client_gg.get_table(view)
        client_gg.update_table(view, ['view_query'])
    except:
        client_gg.create_table(view)

def load_data_sql_to_datamart():

    view_id = 'factory-hackathon-2023.airflow.mysql_db_query_1'
    view = bigquery.Table(view_id)
    # TOP 10 BRAND and PRICE
    view.view_query = "SELECT branding as BRAND, count(1) as COUNT, avg(current_price) as AVG_PRICE \
    FROM `factory-hackathon-2023.airflow.mysql_data` \
    GROUP BY branding \
    ORDER BY count(1) DESC \
    LIMIT 20"

    try:
        client_gg.get_table(view)
        client_gg.update_table(view, ['view_query'])
    except:
        client_gg.create_table(view)

# extract_data_mongo_to_cgs(records=1000)
# extract_data_mysql_to_cgs()
# transfrom_data_to_bigquery()
# load_data_to_datamart()