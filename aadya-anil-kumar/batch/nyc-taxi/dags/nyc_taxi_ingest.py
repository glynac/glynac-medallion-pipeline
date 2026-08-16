import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
#from clickhouse_driver import Client

# ── CONFIG ────────────────────────────────────────────────────────────
DAG_ID = "nyc_taxi_ingest"
PARQUET_PATH = "/opt/airflow/data/yellow_tripdata_2024-01.parquet"
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9009))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "medallion")

default_args = {
    "owner": "aadya-anil-kumar",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# ── HELPERS ───────────────────────────────────────────────────────────
'''
def get_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB
    )
'''
# ── TASK 1: FILE CHECK ────────────────────────────────────────────────
def file_check():
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"Parquet file not found at: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
    print(f"✅ File found. Rows: {len(df)}, Columns: {df.columns.tolist()}")

# ── TASK 2: VALIDATE SCHEMA ───────────────────────────────────────────
def validate_schema():
    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
    expected = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'RatecodeID',
        'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
        'payment_type', 'fare_amount', 'extra', 'mta_tax',
        'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge', 'Airport_fee'
    ]
    missing = set(expected) - set(df.columns.tolist())
    if missing:
        raise ValueError(f"❌ Missing columns: {missing}")
    print(f"✅ Schema valid. {len(df)} rows.")

# ── TASK 3: LOAD BRONZE ───────────────────────────────────────────────
def load_bronze():
    import requests
    
    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")

    # Convert timestamps
    df['tpep_pickup_datetime'] = pd.to_datetime(
        df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(
        df['tpep_dropoff_datetime'], errors='coerce')

    # Drop rows with null datetimes
    df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    # Convert to string
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Fill nulls
    df = df.fillna(0)
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str).replace('0', 'N')

    # Insert via HTTP API in batches
    batch_size = 5000
    total = len(df)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size]
        
        # Build CSV data for ClickHouse HTTP insert
        rows = []
        for row in batch.itertuples(index=False):
            rows.append('\t'.join([
                '',  # ingested_at — default
                str(int(row.VendorID)) if row.VendorID else '0',
                str(row.tpep_pickup_datetime),
                str(row.tpep_dropoff_datetime),
                str(int(float(row.passenger_count))) if row.passenger_count else '0',
                str(float(row.trip_distance)) if row.trip_distance else '0',
                str(int(float(row.RatecodeID))) if row.RatecodeID else '0',
                str(row.store_and_fwd_flag),
                str(int(row.PULocationID)) if row.PULocationID else '0',
                str(int(row.DOLocationID)) if row.DOLocationID else '0',
                str(int(float(row.payment_type))) if row.payment_type else '0',
                str(float(row.fare_amount)) if row.fare_amount else '0',
                str(float(row.extra)) if row.extra else '0',
                str(float(row.mta_tax)) if row.mta_tax else '0',
                str(float(row.tip_amount)) if row.tip_amount else '0',
                str(float(row.tolls_amount)) if row.tolls_amount else '0',
                str(float(row.improvement_surcharge)) if row.improvement_surcharge else '0',
                str(float(row.total_amount)) if row.total_amount else '0',
                str(float(row.congestion_surcharge)) if row.congestion_surcharge else '0',
                str(float(row.Airport_fee)) if row.Airport_fee else '0',
            ]))
        
        data = '\n'.join(rows)
        
        response = requests.post(
            f'http://{CLICKHOUSE_HOST}:8123/',
            params={'query': 'INSERT INTO medallion.bronze_nyc_taxi FORMAT TabSeparated'},
            data=data.encode('utf-8')
        )
        
        if response.status_code != 200:
            raise Exception(f"ClickHouse insert failed: {response.text}")
        
        inserted += len(batch)
        print(f"✅ Inserted {inserted}/{total} rows")

    print(f"✅ Bronze load complete. Total: {total} rows")

# ── DAG DEFINITION ────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="NYC Taxi CSV → Bronze ClickHouse",
    schedule_interval="@once",
    catchup=False,
    tags=["nyc-taxi", "bronze", "clickhouse", "medallion"],
) as dag:

    t1 = PythonOperator(task_id="file_check", python_callable=file_check)
    t2 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t3 = PythonOperator(task_id="load_bronze", python_callable=load_bronze)

    t1 >> t2 >> t3