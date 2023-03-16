import datetime

import requests
import os
import pandas as pd

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.models import Variable


lat = 50.4501
lng = 30.5234

# airflow variable
wwo_key = Variable.get("KEY_API_WWO")

def extract_data_kyiv(**kwargs):
    ti = kwargs['ti']
    # Запрос на прогноз со следующего часа
    response = requests.get(
            'http://api.worldweatheronline.com/premium/v1/weather.ashx',
            params={
                'q':'{},{}'.format(lat,lng),
                'tp':'1',
                'num_of_days':1,
                'format':'json',
                'key':wwo_key
            },
            headers={
                'Authorization': wwo_key
            }
        )

    if response.status_code==200:
        json_data = response.json()
        print(json_data)

        ti.xcom_push(key='wwo_kyiv_json', value=json_data)

def transform_data():
    pass

def load_data():
    pass



args = {
    'owner' : 'Oleksii',
    'start_date': datetime.datetime(2023, 3, 15),
    'provide_context': True,
    'email':['a.lepilo.soft@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG('load_weater_data',
         description='loading data from wwo api',
         schedule='@daily',
         default_args=args
        ) as dag:
    extract_data = PythonOperator(task_id='extract_data_kyiv', python_callable=extract_data_kyiv)
    
    extract_data