import requests
import datetime
import polars as pl
from pathlib import Path

api_token = 'dAAvvGs68Iwh2wUHcljy46AvNJ6mUo7o59dTLHx9' #hehe
base_url = 'https://api.stockdata.org/v1/data/eod'

symbols = ['TSLA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'FB', 'NFLX', 'NVDA', 'BABA', 'INTC']

end_date = datetime.datetime.now().date()
start_date = end_date - datetime.timedelta(days=20) #aq n entendi pq tive que colocar 20 dias pra voltar 10 linhas
print(f"Start date: {start_date}, End date: {end_date}")

for symbol in symbols:
    print(f"Fetching data for {symbol}...")
    response = requests.get(
        base_url,
        params={
            'symbols': symbol,
            'api_token': api_token,
            'date_from': start_date,
            'date_to': end_date
        }
    )

    if response.status_code == 200:
        data = response.json()
        df = pl.DataFrame(data['data'])

        if df.is_empty():
            print(f"No data found for {symbol}. Skipping...")
            continue

        df = df.with_columns(pl.lit(symbol).alias('symbol'))

        file_path = Path('/home/thiagouehara/Documentos/ex-lucas/quero-data-team-training/tdd_api/data/full_load/raw_data.parquet')
        print(f'File path: {file_path}')

        if file_path.exists():
            print(f"File {file_path} already exists. Appending data...")

            existing_df = pl.read_parquet(file_path)
            df = pl.concat([existing_df, df])
            df.write_parquet(file_path)
        else:

            print(f"File {file_path} does not exist. Creating new file...")
            
            df.write_parquet(file_path)
        
        print(f'Data for {symbol}:')
        print(df)
    else:
        print(f"Failed to fetch data for {symbol}: {response.status_code}")