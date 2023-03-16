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
ua_timezone = 2

# airflow variable
wwo_key = Variable.get("KEY_API_WWO")

def extract_data_kyiv(**kwargs):
    ti = kwargs['ti']
    # Запрос на прогноз со следующего часа
    response = requests.get(
            'http://api.worldweatheronline.com/premium/v1/weather.ashx',
            params={
                'q':'Kiev',
                'tp':'24',
                'num_of_days':1,
                'format':'json',
                'key':wwo_key,
                'showlocaltime':'yes'
            },
            headers={
                'Authorization': wwo_key
            }
        )

    if response.status_code==200:
        json_data = response.json()
        print(json_data)

        ti.xcom_push(key='wwo_kyiv_json', value=json_data)



def transform_data_kyiv(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='wwo_kyiv_json', task_ids=['extract_data_kyiv'])[0]
    request_data = json_data['data']['request']
    time_data = json_data['data']['time_zone']
    weather_data = json_data['data']['current_condition']
# 2023-03-16 20:11
    timestamp = datetime.datetime.strptime(time_data[0]['localtime'], '%Y-%m-%d %H:%M')
    """ 
    print(timestamp)
    date = timestamp.date()
    print(date)
    time = timestamp.time()
    print(time)
     """
    js = {
        'date': str(timestamp.date()),
        'time': str(timestamp.time()),
        'city': request_data[0]['query'],
        'weather': weather_data[0]['weatherDesc'][0]['value'],
        'temp_C': weather_data[0]['temp_C'],
        'temp_C_feels_like': weather_data[0]['FeelsLikeC'],
        'wind_speed_kmph': weather_data[0]['windspeedKmph'],
        'wind_from': weather_data[0]['winddir16Point'],
        'humidity': weather_data[0]['humidity'],
        'pressure': weather_data[0]['pressure'],
        'cloudcover': weather_data[0]['cloudcover'],
        
    }

    ti.xcom_push(key='weather_kyiv_json_filtered', value=js)



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

    transform_data  = PythonOperator(task_id='transform_data_kyiv', python_callable=transform_data_kyiv)
    
    extract_data >> transform_data