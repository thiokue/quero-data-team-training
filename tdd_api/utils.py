import polars as pl
import os
from datetime import datetime

def load_latest_parquet(bronze_folder):
    files = os.listdir(bronze_folder)
    parquet_files = [f for f in files if f.endswith('.parquet')]
    dates = [datetime.strptime(f.split('.')[0], '%Y-%m-%d') for f in parquet_files]
    latest_date = max(dates)
    latest_file = latest_date.strftime('%Y-%m-%d') + '.parquet'
    latest_file_path = os.path.join(bronze_folder, latest_file)
    df = pl.read_parquet(latest_file_path)
    return df, latest_date