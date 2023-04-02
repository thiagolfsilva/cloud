import requests
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

# TODO: Set project_id to your Google Cloud Platform project ID.
# project_id = "my-project"

project_id='data-warehouse-course-ps'

sql = """
SELECT *
FROM test_set.ftx_perpetual
WHERE future IN ('ATOM-PERP', 'BTC-PERP')
ORDER BY future,time;
"""

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

df = pandas_gbq.read_gbq(sql, project_id=project_id)
print(df)
#df_unix_sec = pd.to_datetime(df['time']).astype(int)/ 10**9
#print(df_unix_sec[0])
#print(type(df['time'][0]))