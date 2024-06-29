import requests
import os
from dotenv import load_dotenv

list_of_crypto = ['ethereum', 'bitcoin', 'solana', 'dogecoin', 'arbitrum', 'binancecoin', 'ripple', 'shiba-inu', 'dai', 'matic-network', 'tron', 'avalanche-2']

headers = { 'x-cg-demo-api-key': os.getenv("MY_API_KEY")}

for crypto in list_of_crypto:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {  
        "ids": crypto,
        "vs_currencies": "USD",
        "include_market_cap":'true', 
        "include_24hr_vol":'true', 
        "include_24hr_change":'true', 
        "include_last_updated_at":'true'
    }

    # Replace 'YOUR_API_KEY' with your actual API key

    response = requests.get(url, params= params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(data)