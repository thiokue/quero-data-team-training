import polars as pl
from utils import load_latest_parquet

bronze_folder = '/home/thiagouehara/Documentos/ex-lucas/quero-data-team-training/tdd_api/data/bronze'
df, latest_date = load_latest_parquet(bronze_folder)

# Make all column names lowercase
df = df.rename({col: col.lower() for col in df.columns})

# Ensure types in the columns (example: converting to specific types)
# Adjust the types as needed
df = df.with_columns([
    pl.col('date').str.strptime(pl.Date, format='%Y-%m-%dT%H:%M:%S.%fZ').alias('date'),
    pl.col('open').cast(pl.Float64),
    pl.col('high').cast(pl.Float64),
    pl.col('low').cast(pl.Float64),
    pl.col('close').cast(pl.Float64),
    pl.col('volume').cast(pl.Int64)
])

print(f'Latest date: {latest_date}')
print(f"Dataframe loaded with shape: {df.shape}")
print(df)

df.write_parquet(f"/home/thiagouehara/Documentos/ex-lucas/quero-data-team-training/tdd_api/data/silver/{latest_date.strftime('%Y-%m-%d')}.parquet")