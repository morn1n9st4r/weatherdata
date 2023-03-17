import datetime

import requests
import os
import pandas as pd

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.models import Variable

wwo_key = Variable.get("KEY_API_WWO")

cities = {'north' : ['Kiev', 'Chernihiv', 'Sumy', 'Poltava', 'Zhytomyr', 'Cherkasy'],
          'south' : ['Vinnytsya', 'Kirovohrad', 'Mykolayiv', 'Odesa', 'Kherson', 'Simferopol'],
          'east' : ['Zaporizhzhya', 'Dnipropetrovsk', 'Kharkiv', 'Luhansk', 'Donetsk_Ukraine'],
          'west' : ['Lutsk_Ukraine', 'Uzhhorod', 'Ivanofrankivsk', 'Lviv', 'Rivne',
                    'Ternopil', 'Khmelnytskyy', 'Chernivtsi']}

regions = ['north','south','east', 'west']

def extract_data(city, **kwargs):

    """
    Extracts raw weather data from World Weather Online API for a specified city, and pushes it to XCom
    for downstream tasks to use.
    
    Args:
        city (str): Name of the city for which to extract weather data.
        **kwargs: Arbitrary keyword arguments that include a TaskInstance (ti) object.
        
    Returns:
        None
    """

    ti = kwargs['ti']

    # Send a GET request to the World Weather Online API for weather data for the specified city
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

    # Check if the response status code is 200 (OK)
    if response.status_code==200:
        json_data = response.json()
        print(json_data)

        ti.xcom_push(key=f'wwo_{city}_json', value=json_data)



def transform_data(city, **kwargs):

    """
    Transforms raw weather data extracted from World Weather Online API into a filtered dictionary,
    and pushes it to XCom for downstream tasks to use.
    
    Args:
        city (str): Name of the city for which to transform the weather data.
        **kwargs: Arbitrary keyword arguments that include a TaskInstance (ti) object with an XCom
                  key for the raw weather data extracted by the "extract_data_{city}" task.
                  
    Returns:
        None
    """

    # Extract the TaskInstance (ti) object from the keyword arguments
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key=f'wwo_{city}_json', task_ids=[f'extract_data_{city}'])[0]

    # Extract the relevant data from the raw weather data JSON
    request_data = json_data['data']['request']
    time_data = json_data['data']['time_zone']
    weather_data = json_data['data']['current_condition']

    timestamp = datetime.datetime.strptime(time_data[0]['localtime'], '%Y-%m-%d %H:%M')

    # Construct a filtered dictionary with the relevant weather data
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


def agg_region(cities, region, **kwargs):

    """
    Aggregates weather data for multiple cities into a single region and pushes it to XCom for downstream tasks to use.
    
    Args:
        cities (list): List of city names for which to aggregate weather data.
        region (str): Name of the region to which the cities belong.
        **kwargs: Arbitrary keyword arguments that include a TaskInstance (ti) object.
        
    Returns:
        None
    """

    ti = kwargs['ti']

    # Extract time data from JSON data for the first city in the list
    json_data = ti.xcom_pull(key=f'wwo_{cities[0]}_json', task_ids=[f'extract_data_{cities[0]}'])[0]
    time_data = json_data['data']['time_zone']
   
    # Extract timestamp from time data and initialize the aggregated data dictionary
    timestamp = datetime.datetime.strptime(time_data[0]['localtime'], '%Y-%m-%d %H:%M')
    agg = {
        'date': str(timestamp.date()),
        'time': str(timestamp.time()),
        'region' : region,
        'temp_avg' : 0,
        'humidity_avg' : 0,
        'cloudcover_avg' : 0
    }

    # Iterate over each city in the list and aggregate weather data for the region
    for city in cities:
        # Pull filtered weather data for the city from XCom
        json_data = ti.xcom_pull(key=f'weather_{city}_json_filtered', task_ids=[f'transform_data_{city}'])[0]
        # Update aggregated data with average temperature, humidity, and cloud cover for the region
        agg['temp_avg'] += int(json_data['temp_C'])
        agg['humidity_avg'] += int(json_data['humidity'])
        agg['cloudcover_avg'] += int(json_data['cloudcover'])

    # Calculate the average temperature, humidity, and cloud cover for the region
    agg['temp_avg'] /= len(cities)
    agg['humidity_avg'] /= len(cities)
    agg['cloudcover_avg'] /= len(cities)


    ti.xcom_push(key=f'weather_{region}_json_filtered', value=agg)



def create_insert_query(city, region):
    query = f"""
    INSERT INTO weather_values VALUES (
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["date"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["time"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["city"] }}}}',
        '{region}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["weather"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["temp_C"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["temp_C_feels_like"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["wind_speed_kmph"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["wind_from"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["humidity"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["pressure"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{city}_json_filtered", task_ids=["transform_data_{city}"])[0]["cloudcover"] }}}}'
    )
    """
    return query

def create_insert_region_query(region):
    query = f"""
    INSERT INTO regions_weather_values VALUES (
        '{{{{ ti.xcom_pull(key="weather_{region}_json_filtered", task_ids=["aggregate_{region}"])[0]["region"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{region}_json_filtered", task_ids=["aggregate_{region}"])[0]["date"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{region}_json_filtered", task_ids=["aggregate_{region}"])[0]["time"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{region}_json_filtered", task_ids=["aggregate_{region}"])[0]["temp_avg"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{region}_json_filtered", task_ids=["aggregate_{region}"])[0]["humidity_avg"] }}}}',
        '{{{{ ti.xcom_pull(key="weather_{region}_json_filtered", task_ids=["aggregate_{region}"])[0]["cloudcover_avg"] }}}}'
    )
    """
    return query

args = {
    'owner' : 'Oleksii',
    'start_date': datetime.datetime(2023, 3, 15),
    'provide_context': True,
    'email':['a.lepilo.soft@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG('load_weather_dag',
         description='loading data from wwo api and calculate averages by regions',
         schedule_interval='0 */2 * * *',
         default_args=args,
         catchup=False
        ) as dag:

    # Create table for weather_values
    create_weather_table = PostgresOperator(
                                task_id="create_weather_table",
                                postgres_conn_id="postgres_weather",
                                sql="""
                                    CREATE TABLE IF NOT EXISTS weather_values (
                                    date DATE NOT NULL,
                                    time TIME NOT NULL,
                                    city VARCHAR NOT NULL,
                                    region VARCHAR NOT NULL,
                                    weather VARCHAR NOT NULL,
                                    temp_C VARCHAR NOT NULL,
                                    temp_C_feels_like VARCHAR NOT NULL,
                                    wind_speed_kmph FLOAT NOT NULL,
                                    wind_from VARCHAR NOT NULL,
                                    humidity FLOAT NOT NULL,
                                    pressure FLOAT NOT NULL,
                                    cloudcover FLOAT NOT NULL);
                                """,
                                )

    # Create table for regions_weather_values
    crt = PostgresOperator( task_id="create_regions_table",
                                postgres_conn_id="postgres_weather",
                                sql="""
                                    CREATE TABLE IF NOT EXISTS regions_weather_values (
                                    region VARCHAR NOT NULL,
                                    date DATE NOT NULL,
                                    time TIME NOT NULL,
                                    temp_C_avg VARCHAR NOT NULL,
                                    humidity_avg FLOAT NOT NULL,
                                    cloudcover_avg FLOAT NOT NULL);
                                """,
                                )

    # Loop over all regions
    for region in regions:
        
        # Aggregate data for each region
        agg_reg = PythonOperator(task_id=f'aggregate_{region}', python_callable=agg_region, op_kwargs={'cities': cities[region], 'region': region})
        
        # Push aggregated data for each region to regions_weather_values table
        push_regions_data_tsk = PostgresOperator(
                        task_id=f"insert_regions_weather_table_{region}",
                        postgres_conn_id="postgres_weather",
                        sql=create_insert_region_query(region))

        # Loop over all cities in each region
        for city in cities[region]:

            # Extract weather data for each city
            extract_data_tsk = PythonOperator(task_id=f'extract_data_{city}', python_callable=extract_data, op_kwargs={'city': city})
            
            # Transform weather data for each city
            transform_data_tsk  = PythonOperator(task_id=f'transform_data_{city}', python_callable=transform_data, op_kwargs={'city': city})
            
            # Push transformed data for each city to weather_values table
            push_data_tsk = PostgresOperator(
                task_id=f"insert_weather_table_{city}",
                postgres_conn_id="postgres_weather",
                sql=create_insert_query(city, region))

            # Define task dependencies
            extract_data_tsk >> transform_data_tsk >>  create_weather_table >> push_data_tsk >> agg_reg >> crt >> push_regions_data_tsk
    
