from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import pendulum

dags_dir = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(dags_dir, os.pardir))
connections_dir = os.path.join(parent_directory, 'connections')
connections_dir = os.path.join(parent_directory, 'etl')
sys.path.append(connections_dir)

from etl import extract_data_mongo_to_cgs, extract_data_mysql_to_cgs, transfrom_data_mongo_to_bigquery, transfrom_data_mysql_to_bigquery, load_data_to_datamart

default_args = {
    'owner': 'MinhTr',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def task_1():
    print('a')

with DAG(
        default_args=default_args,
        dag_id='minh_dags_v7',
        description='This is my first ETL',
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        schedule_interval=None,
) as dag:
    extract_data_mongo = PythonOperator(
        task_id='extract_data_mongo',
        python_callable=extract_data_mongo_to_cgs,
        op_kwargs={'records': 2000}
    )
    extract_data_mysql = PythonOperator(
        task_id='extract_data_mysql',
        python_callable=extract_data_mysql_to_cgs
    )
    transfrom_data_mysql = PythonOperator(
        task_id='transfrom_data_mysql',
        python_callable=transfrom_data_mysql_to_bigquery
    )
    transfrom_data_mongo = PythonOperator(
        task_id='transfrom_data_mongo',
        python_callable=transfrom_data_mongo_to_bigquery
    )
    load_data_mongo = PythonOperator(
        task_id='load_data_mongo',
        python_callable=load_data_to_datamart
    )
    extract_data_mongo >> transfrom_data_mongo >> load_data_mongo
    extract_data_mysql >> transfrom_data_mysql >> load_data_mongo


