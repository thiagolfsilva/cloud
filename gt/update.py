import requests
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

project_id='data-warehouse-course-ps'

sql_latest = """
SELECT timestamp
FROM test_set.gt_perpetual
ORDER BY timestamp DESC
LIMIT 1;
"""

project_id = 'data-warehouse-course-ps'
dataset_id = 'test_set'
table_name = 'gt_perpetual'

#Appends all the data from the latest available in the dataset to now
def update():
    now = datetime.now().timestamp()
    df = pandas_gbq.read_gbq(sql_latest, project_id=project_id)
    latest=df['timestamp'][0]
    print(now-latest)
    interval = 3600*8

    if (now-latest) < interval:
        print("up to date")

    else:
        limit = int((now-latest)/interval)
        print(f"{limit} intervals behind")

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
            query_param = 'contract='+name+'&limit='+str(limit)
            r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
            for entry in r.json():
                base, quote = name.split('_')
                row = [base, quote, entry['r'], entry['t']]
                batch_data.append(row)
        
        df = pd.DataFrame(batch_data, columns = ['base','quote','rate','timestamp'])
        df.to_gbq(destination_table='{}.{}'.format(dataset_id, table_name), project_id=project_id,if_exists='append')

if __name__ == "__main__":
    update()