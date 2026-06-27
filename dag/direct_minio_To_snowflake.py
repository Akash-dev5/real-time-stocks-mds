import json
import logging
import boto3
import snowflake.connector
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

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

# ──────────────────────────────────────────────────────────────────────────────
# ONE-TIME SNOWFLAKE SETUP  (run this manually in Snowflake once, not in code)
# Get credentials from Airflow UI → Admin → Connections → minio_conn
# ──────────────────────────────────────────────────────────────────────────────
#
#   CREATE OR REPLACE STAGE minio_external_stage
#       URL         = 's3://bronze-transactions/'
#       CREDENTIALS = (AWS_KEY_ID='<MINIO_KEY>' AWS_SECRET_KEY='<MINIO_SECRET>')
#       ENDPOINT    = '<MINIO_HOST>'
#       FILE_FORMAT = (TYPE = JSON);
#
#   CREATE TABLE IF NOT EXISTS bronze_stock_quotes_raw (
#       raw_data  VARIANT,
#       loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#   );
#
# ──────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="minio_to_snowflake",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2025, 9, 9),
    schedule="@hourly",
    catchup=False,
    tags=["bronze", "stocks"],
)
def minio_to_snowflake_dag():

    @task
    def get_unprocessed_files():
        """Only fetch files not yet processed — avoids duplicates"""
        s3 = get_minio_client()
        bucket = "bronze-transactions"

        # get all files in bucket (skip the diary file itself)
        response = s3.list_objects_v2(Bucket=bucket)
        all_files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if not obj["Key"].startswith("_processed/")
        ]

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
    def load_to_snowflake(new_files: list):
        """
        Load files directly from MinIO into Snowflake via External Stage.
        No /tmp download needed — Snowflake fetches from MinIO itself.

        Flow:
            MinIO (External Stage) ── COPY INTO ──► Snowflake Table
        """
        if not new_files:
            logger.info("No new files to load into Snowflake")
            return []

        conn = get_snowflake_conn()
        cur = conn.cursor()
        loaded_files = []

        try:
            for key in new_files:
                try:
                    cur.execute(f"""
                        COPY INTO bronze_stock_quotes_raw (raw_data)
                        FROM @minio_external_stage/{key}
                        FILE_FORMAT = (TYPE = JSON)
                        ON_ERROR    = 'CONTINUE'
                        FORCE       = FALSE
                    """)
                    result = cur.fetchone()
                    rows_loaded = result[3] if result else 0
                    logger.info(f"Loaded '{key}' → {rows_loaded} rows")
                    loaded_files.append(key)

                except Exception as e:
                    logger.error(f"Failed to load '{key}': {e}")

            return loaded_files  # only successfully loaded files

        finally:
            cur.close()   # always close — even if error occurs
            conn.close()

    @task
    def mark_files_processed(loaded_files: list):
        """Track processed files so we don't process them again"""
        if not loaded_files:
            logger.info("No files to mark as processed")
            return

        s3 = get_minio_client()
        bucket = "bronze-transactions"

        # load existing processed list
        try:
            processed_obj = s3.get_object(Bucket=bucket, Key="_processed/processed_keys.json")
            processed_files = json.loads(processed_obj["Body"].read())
        except Exception:
            processed_files = []

        # add newly loaded files
        processed_files.extend(loaded_files)

        # save back to MinIO
        s3.put_object(
            Bucket=bucket,
            Key="_processed/processed_keys.json",
            Body=json.dumps(processed_files),
            ContentType="application/json"
        )
        logger.info(f"Marked {len(loaded_files)} files as processed. Total: {len(processed_files)}")

    # Task dependencies — same structure as before
    new_files    = get_unprocessed_files()
    loaded_files = load_to_snowflake(new_files)
    mark_files_processed(loaded_files)

minio_to_snowflake_dag()
