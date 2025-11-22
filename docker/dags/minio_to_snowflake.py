import os
import json
import boto3
import tempfile
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Load environment variables
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")


# ---------------- MinIO Client -----------------

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# ---------------- Create RAW Table -----------------

def create_table_if_not_exists():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    sql = f"""
    USE DATABASE {SNOWFLAKE_DB};
    USE SCHEMA {SNOWFLAKE_SCHEMA};

    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE}  (
    rank INTEGER,
    track_id STRING,
    track_name STRING,
    artist_id STRING,
    artist_name STRING,
    album_name STRING,
    popularity INTEGER,
    duration_ms INTEGER,
    ingested_at FLOAT,
    file_name STRING,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

    """

    hook.run(sql)
    print(f"Table ensured: {SNOWFLAKE_TABLE}")


# ---------------- List MinIO Files -----------------

def list_minio_objects():
    client = get_minio_client()
    response = client.list_objects_v2(Bucket=MINIO_BUCKET)
    contents = response.get("Contents", [])

    keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".json")]

    print(f"Found {len(keys)} JSON files in bucket {MINIO_BUCKET}")
    return keys


# ---------------- Load MinIO → Snowflake -----------------

def load_data_to_snowflake(ti, **context):
    files = ti.xcom_pull(task_ids='list_minio_objects')

    if not files:
        print("No JSON files found.")
        return

    minio = get_minio_client()
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            rank,
            track_id,
            track_name,
            artist_id,
            artist_name,
            album_name,
            popularity,
            duration_ms,
            ingested_at,
            file_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for key in files:
        print(f"Processing file: {key}")

        # Read file from MinIO
        with tempfile.NamedTemporaryFile("wb+", delete=False) as tmp:
            minio.download_fileobj(MINIO_BUCKET, key, tmp)
            tmp.seek(0)
            lines = tmp.read().decode("utf-8").splitlines()

        # JSON Lines → parse each object
        for line in lines:
            record = json.loads(line)

            params = [
                record.get("rank"),
                record.get("track_id"),
                record.get("track_name"),
                record.get("artist_id"),
                record.get("artist_name"),
                record.get("album_name"),
                record.get("popularity"),
                record.get("duration_ms"),
                record.get("ingested_at"),
                key
            ]

            hook.run(insert_sql, parameters=params)

        print(f"Loaded {len(lines)} records from {key}")




# ---------------- DAG Definition -----------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_raw_data",
    default_args=default_args,
    description="Load raw Spotify JSON from MinIO to Snowflake RAW",
    schedule="@hourly",
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    list_objects = PythonOperator(
        task_id="list_minio_objects",
        python_callable=list_minio_objects,
    )

    load_data = PythonOperator(
        task_id="load_data_to_snowflake",
        python_callable=load_data_to_snowflake,
    )

    create_table >> list_objects >> load_data
