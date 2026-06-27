import os
import json
import logging
import boto3
import snowflake.connector
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

LOCAL_DIR = "/tmp/minio_downloads"

def get_minio_client():
    """Get MinIO client using Airflow Connection — no hardcoded credentials"""
    conn = BaseHook.get_connection("minio_conn")  # stored in Airflow UI
    return boto3.client(
        "s3",
        endpoint_url=conn.host,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

def get_snowflake_conn():
    """Get Snowflake connection using Airflow Connection"""
    conn = BaseHook.get_connection("snowflake_conn")  # stored in Airflow UI
    return snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema
    )

@dag(
    dag_id="minio_to_snowflake",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2025, 9, 9),
    schedule="@hourly",        # every hour, not every minute
    catchup=False,
    tags=["bronze", "stocks"], # helps organize in Airflow UI
)
def minio_to_snowflake_dag():

    @task
    def get_unprocessed_files(**kwargs):
        """Only fetch files not yet processed — avoids duplicates"""
        s3 = get_minio_client()
        bucket = "bronze-transactions"

        # get all files in bucket
        response = s3.list_objects_v2(Bucket=bucket)
        all_files = [obj["Key"] for obj in response.get("Contents", [])]

        # get already processed files (stored in a tracking file in MinIO)
        try:
            processed_obj = s3.get_object(Bucket=bucket, Key="_processed/processed_keys.json")
            processed_files = json.loads(processed_obj["Body"].read())
        except Exception:
            processed_files = []  # first run — nothing processed yet

        # only return NEW files
        new_files = [f for f in all_files if f not in processed_files]
        logger.info(f"Found {len(new_files)} new files to process")
        return new_files

    @task
    def download_files(new_files: list):
        """Download only new files to local /tmp folder"""
        if not new_files:
            logger.info("No new files to download")
            return []

        os.makedirs(LOCAL_DIR, exist_ok=True)
        s3 = get_minio_client()
        bucket = "bronze-transactions"
        local_files = []

        for key in new_files:
            try:
                local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
                s3.download_file(bucket, key, local_file)
                local_files.append(local_file)
                logger.info(f"Downloaded {key} -> {local_file}")
            except Exception as e:
                logger.error(f"Failed to download {key}: {e}")
                raise  # fail loudly so Airflow retries

        return local_files

    @task
    def load_to_snowflake(local_files: list):
        """Load downloaded files into Snowflake"""
        if not local_files:
            logger.info("No files to load into Snowflake")
            return

        try:
            conn = get_snowflake_conn()
            cur = conn.cursor()

            for f in local_files:
                cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw AUTO_COMPRESS=TRUE")
                logger.info(f"Staged file: {f}")

            cur.execute("""
                COPY INTO bronze_stock_quotes_raw
                FROM @%bronze_stock_quotes_raw
                FILE_FORMAT = (TYPE=JSON)
                ON_ERROR = 'CONTINUE'
            """)
            logger.info("COPY INTO Snowflake executed successfully")

        except Exception as e:
            logger.error(f"Snowflake load failed: {e}")
            raise
        finally:
            cur.close()   # always close — even if error occurs
            conn.close()

    @task
    def mark_files_processed(new_files: list):
        """Track processed files so we don't process them again"""
        if not new_files:
            return

        s3 = get_minio_client()
        bucket = "bronze-transactions"

        # load existing processed list
        try:
            processed_obj = s3.get_object(Bucket=bucket, Key="_processed/processed_keys.json")
            processed_files = json.loads(processed_obj["Body"].read())
        except Exception:
            processed_files = []

        # add newly processed files
        processed_files.extend(new_files)

        # save back to MinIO
        s3.put_object(
            Bucket=bucket,
            Key="_processed/processed_keys.json",
            Body=json.dumps(processed_files),
            ContentType="application/json"
        )
        logger.info(f"Marked {len(new_files)} files as processed")

    # Task dependencies
    new_files = get_unprocessed_files()
    downloaded = download_files(new_files)
    load_to_snowflake(downloaded)
    mark_files_processed(new_files)

minio_to_snowflake_dag()
