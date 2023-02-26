Overview

This repository is composed of cloud functions that interact with crypto exchange APIs to query historical data on funding rates, and load it into a Google Bigquery Dataset.

There is a folder for each exchange. In each:
- initialize.py batch loads initial data on the Google BigQuery Platform
- update.py updates the data
- reader.py gives a few example SQL Queries

