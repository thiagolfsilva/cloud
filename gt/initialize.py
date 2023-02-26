import requests
import json
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

project_id = 'data-warehouse-course-ps'
dataset_id = 'test_set'
table_name = 'gt_perpetual'

#Initializes the database with one hour worth of data
def initialize():

    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    url = '/futures/usdt/contracts'
    query_param = ''
    r = requests.request('GET', host + prefix + url, headers=headers)
    all_futures=r.json()

    batch_data=[]
    url = '/futures/usdt/funding_rate'

    for future in all_futures:
        name=future['name']
        print(name)
        query_param = 'contract='+name+'&limit=1000'
        r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
        for entry in r.json():
            base, quote = name.split('_')
            row = [base, quote, entry['r'], entry['t']]
            batch_data.append(row)
    
    df = pd.DataFrame(batch_data, columns = ['base','quote','rate','timestamp'])
    df.to_gbq(destination_table='{}.{}'.format(dataset_id, table_name), project_id=project_id,if_exists='append')

if __name__ == "__main__":
    initialize()