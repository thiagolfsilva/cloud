import requests
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

project_id='data-warehouse-course-ps'

sql_oldest = """
SELECT time
FROM test_set.ftx_perpetual_futures
ORDER BY time
LIMIT 1;
"""

sql_latest = """
SELECT time
FROM test_set.ftx_perpetual_futures
ORDER BY time DESC
LIMIT 1;
"""
#df = pandas_gbq.read_gbq(sql_oldest, project_id=project_id)
#print(df)
#df_unix_sec = pd.to_datetime(df['time']).astype(int)/ 10**9
#timestamp=df_unix_sec[0]


#params = {'start_time':str(timestamp-3700), 'end_time':str(timestamp-100)}
#response = requests.get("https://ftx.com/api/funding_rates", params=params)
#data = pd.json_normalize(response.json()["result"])
#print(data)

project_id = 'data-warehouse-course-ps'
dataset_id = 'test_set'
table_name = 'ftx_perpetual'

#Set arbitrarily
start=1666975000

#Initializes the database with one hour worth of data
def initialize(timestamp):
    params = {'start_time':str(timestamp-3600), 'end_time':str(timestamp)}
    response = requests.get("https://ftx.com/api/funding_rates", params=params)
    data = pd.json_normalize(response.json()["result"])

    data.to_gbq(destination_table='{}.{}'.format(dataset_id, table_name), project_id=project_id,if_exists='append')

#Gets what is the oldest timestamp in the dataset
#Query the data for one hour before that and appends it
def add_one_before():
    df = pandas_gbq.read_gbq(sql_oldest, project_id=project_id)
    oldest=df['time'][0].timestamp()    
    
    params = {'start_time':str(oldest-3700), 'end_time':str(oldest-100)}
    response = requests.get("https://ftx.com/api/funding_rates", params=params)
    data = pd.json_normalize(response.json()["result"])

    data.to_gbq(destination_table='{}.{}'.format(dataset_id, table_name), project_id=project_id,if_exists='append')

def add_multiple_before_timestamp(timestamp):
    df = pandas_gbq.read_gbq(sql_oldest, project_id=project_id)
    oldest=df['time'][0].timestamp()
    if timestamp<oldest:
        repetitions=int((oldest-timestamp)/3600)
        for i in range(repetitions):
            add_one_before()

#Appends all the data from the latest available in the dataset to now
def update():
    now = datetime.now().timestamp()
    df = pandas_gbq.read_gbq(sql_latest, project_id=project_id)
    latest=df['time'][0].timestamp()
    while now-latest>3700:
        params = {'start_time':str(latest+100), 'end_time':str(latest+3700)}
        response = requests.get("https://ftx.com/api/funding_rates", params=params)
        data = pd.json_normalize(response.json()["result"])
        data.to_gbq(destination_table='{}.{}'.format(dataset_id, table_name), project_id=project_id,if_exists='append')
        #query dataframe again to get latest OR latest+3600
        latest+=3600

#initialize(start)
#update()