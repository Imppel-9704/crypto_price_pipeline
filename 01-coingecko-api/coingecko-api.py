import requests
import os
import psycopg2
from dotenv import load_dotenv
from typing import NewType

PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_create_statement = """
    CREATE TABLE IF NOT EXISTS crypto_currencies (
        name VARCHAR(50) PRIMARY KEY,
        price DECIMAL(20, 10),
        umarket_cap DECIMAL(20, 2),
        24h_vol DECIMAL(20, 2),
        24h_change DECIMAL(5, 2),
        last_updated_at TIMESTAMP
    )
"""

def create_table(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates table using `table_create_statement`
    """
    cur.execute(table_create_statement)
    conn.commit()

def insert_data(cur: PostgresCursor, conn: PostgresConn, data):
    """
    Inserts data into the `crypto_currencies` table
    """
    for d in data:
        for name, info in d.items():
            insert_statement = """
                INSERT INTO crypto_currencies (
                    name, 
                    price, 
                    market_cap, 
                    24h_vol, 
                    24h_change, 
                    last_updated_at
                ) VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s))
                ON CONFLICT (name) DO NOTHING;
            """
            cur.execute(insert_statement, (name, info['usd'], info['usd_market_cap'], info['usd_24h_vol'], info['usd_24h_change'], info['last_updated_at']))
        conn.commit()

def request_data(coin_name):
    headers = { 'x-cg-demo-api-key': os.getenv("MY_API_KEY")}
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

if __name__ == "__main__":
    list_of_crypto = ['ethereum', 'bitcoin', 'solana', 'notcoin']

    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
        )
        cur = conn.cursor()
        data = request_data(list_of_crypto)
        create_table(cur, conn)
        insert_data(cur, conn, data)

    except Exception as e:
        print(f"Error: {e}")