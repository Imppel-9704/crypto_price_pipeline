# crypto_price_pipeline

This project involves developing a data pipeline using Apache Airflow and Google Cloud Platform. Data is fetched from CoinGecko API using request library, staged in Google Cloud Storage and transferred to Google Bigquery.

![Image](https://drive.google.com/uc?id=1rHrmwju0t-7vrgxn6QTFB5lHuvG2vRFh)


## Getting Started
Fetch docker-compose.yaml
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml'
```

Run docker compose
```
docker compose up
```

## Import Libraries
```
import requests
import os
import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
```