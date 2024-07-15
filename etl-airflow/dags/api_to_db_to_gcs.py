import requests
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone


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

def _create_table():
    table_create_statement = """
        CREATE TABLE IF NOT EXISTS crypto_currencies (
            name VARCHAR(50) PRIMARY KEY,
            price DECIMAL(20, 10),
            market_cap DECIMAL(20, 2),
            h24_vol DECIMAL(20, 2),
            h24_change DECIMAL(5, 2),
            last_updated_at TIMESTAMP
        )
    """

    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(table_create_statement)
    conn.commit()

def _insert_to_table():
    pass

with DAG(
    "api_to_db_to_gcs",
    start_date=timezone.datetime(2024, 7, 15),
    schedule="@daily",
    tags=["project"]
):
    start = EmptyOperator(task_id="start")

    request_data = PythonOperator(
        task_id="request_data",
        python_callable=_request_data,
        op_kwargs={
            "coin_name": ["ethereum", "bitcoin", "solana", "notcoin"],
        }
    )

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=_create_table
    )

    insert_to_table = PythonOperator(
        task_id="insert_to_table",
        python_callable=_insert_to_table
    )
    
    end = EmptyOperator(task_id="end")

    start >> request_data >> create_table >> insert_to_table >> end