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

#cities_str = Variable.get("CITIES")
#cities = cities_str.split(', ')

cities = ['Kiev', 'Chernihiv']

def extract_data(city, **kwargs):
    ti = kwargs['ti']
    # Запрос на прогноз со следующего часа
    response = requests.get(
            'http://api.worldweatheronline.com/premium/v1/weather.ashx',
            params={
                'q':f'{city}',
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

        ti.xcom_push(key=f'wwo_{city}_json', value=json_data)



def transform_data(city, **kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key=f'wwo_{city}_json', task_ids=[f'extract_data_{city}'])[0]
    request_data = json_data['data']['request']
    time_data = json_data['data']['time_zone']
    weather_data = json_data['data']['current_condition']
    
    timestamp = datetime.datetime.strptime(time_data[0]['localtime'], '%Y-%m-%d %H:%M')

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

    ti.xcom_push(key=f'weather_{city}_json_filtered', value=js)



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

    for city in cities:
        extract_data_tsk = PythonOperator(task_id=f'extract_data_{city}', python_callable=extract_data, op_kwargs={'city': city})

        transform_data_tsk  = PythonOperator(task_id=f'transform_data_{city}', python_callable=transform_data, op_kwargs={'city': city})
        
        extract_data_tsk >> transform_data_tsk