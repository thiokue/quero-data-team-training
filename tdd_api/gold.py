import polars as pl
from utils import load_latest_parquet

silver_folder = '/home/thiagouehara/Documentos/ex-lucas/quero-data-team-training/tdd_api/data/silver'
df, latest_date = load_latest_parquet(silver_folder)

# Select the required columns and calculate the price
df = df.select([
    pl.col('date'),
    pl.col('symbol'),
    ((pl.col('high') + pl.col('low')) / 2).alias('price')
])

print(f'Latest date: {latest_date}')
print(f"Dataframe processed with shape: {df.shape}")
print(df)

df.write_parquet(f"/home/thiagouehara/Documentos/ex-lucas/quero-data-team-training/tdd_api/data/gold/{latest_date.strftime('%Y-%m-%d')}.parquet")