import requests
import os
import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def _request_data(coin_name):
    headers = {"x-cg-demo-api-key": os.getenv("MY_API_KEY")}
    list_data = []
    for name in coin_name:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {  
            "ids": name,
            "vs_currencies": "USD",
            "include_market_cap":'true', 
            "include_24hr_vol":'true', 
            "include_24hr_change":'true', 
            "include_last_updated_at":'true'
        }

        response = requests.get(url, params= params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            list_data.append(data)
    return list_data

def _data_to_csv(**context):
    ti = context["ti"]
    list_data = ti.xcom_pull(task_ids="request_data", key="return_value")
    flat_data = []
    for item in list_data:
        for coin, details in item.items():
            if 'last_updated_at' in details:
                date = int(details.pop('last_updated_at'))
                details['last_updated_at'] = datetime.fromtimestamp(date)

            flat_data.append({'coin': coin, **details})

    # Specify CSV column headers
    headers = ['coin', 'usd', 'usd_market_cap', 'usd_24h_vol', 'usd_24h_change', 'last_updated_at']

    date_today = datetime.today().strftime('%Y-%m-%d')
    file_path = f'cryptocurrency_data_{date_today}.csv'
    
    # Write to CSV
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(flat_data)

    return file_path

def _upload_to_gcs(**context):
    ti = context["ti"]
    file = ti.xcom_pull(task_ids="data_to_csv", key="return_value")

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=file,
        dst=file,
        bucket="crypto-data-staging",
        gcp_conn_id="my_gcp_conn"
    )
    upload_file.execute(context=file)

with DAG(
    "api_to_gcs",
    start_date=timezone.datetime(2024, 7, 15),
    schedule=None,
    tags=["project"]
):

    request_data = PythonOperator(
        task_id="request_data",
        python_callable=_request_data,
        op_kwargs={
            "coin_name": ["ethereum", "bitcoin", "solana", "notcoin"],
        }
    )

    data_to_csv = PythonOperator(
        task_id="data_to_csv",
        python_callable=_data_to_csv,
        provide_context=True
    )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs
    )

    date_today = datetime.today().strftime('%Y-%m-%d')
    gcs_to_gbq = GCSToBigQueryOperator(
        task_id="gcs_to_gbq",
        bucket="crypto-data-staging",
        source_objects=f"cryptocurrency_data_{date_today}.csv",
        source_format="CSV",
        destination_project_dataset_table="project_crypto.crypto_data",
        schema_fields=[
        {'name': 'coin', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'usd', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'usd_market_cap', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'usd_24h_vol', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'usd_24h_change', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'last_updated_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="my_gcp_conn"
    )

    request_data >> data_to_csv >> upload_to_gcs >> gcs_to_gbq