import os #for folder creation and file paths
import json #to convert dict to JSON string and back
import logging #for proper logging instead of print
import boto3 #to talk to MinIO (S3)
import snowflake.connector #to talk to Snowflake
from airflow.sdk import dag, task #modern Airflow decorators you already know
from airflow.hooks.base import BaseHook #to fetch credentials stored in Airflow UI
from datetime import datetime, timedelta #for start date and retry delay

logger = logging.getLogger(__name__) 
# __name__ is a Python built-in variable that automatically holds the name of the current file
# If your file is called minio_dag.py → __name__ = "minio_dag"
#So the logger gets named after your file automatically
# This helps when you have multiple DAG files — you can see in logs which file the log came from

# Without __name__ — confusing
# INFO - Downloaded file

# # With __name__ — clear
# INFO - minio_dag - Downloaded file
# INFO - snowflake_dag - Loading to Snowflake
# LOCAL_DIR = "/tmp/minio_downloads"


def get_minio_client():
    """Get MinIO client using Airflow Connection — no hardcoded credentials"""
    conn = BaseHook.get_connection("minio_conn")  # stored in Airflow UI
    return boto3.client(     # ← S3 Client Object created HERE
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

# In Airflow UI when you save a connection, it has fixed standard fields:
# Host     → conn.host
# Login    → conn.login
# Password → conn.password      ← standard field, so direct access
# Schema   → conn.schema
# Port     → conn.port
# But Snowflake needs extra fields like account, warehouse, database that are not standard Airflow fields. So Airflow provides an "Extra" field in the UI where you store them as JSON:
#
# json{
#   "account": "mycompany.us-east-1",
#   "warehouse": "COMPUTE_WH",
#   "database": "STOCKS_MDS"
# }
#
# conn.extra_dejson → reads that Extra JSON field as a Python dictionary
# .get("account") → pulls the specific value out
#
#So your thinking was exactly right — password has one dedicated field, but extra Snowflake-specific things needed a separate JSON storage!


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

# tags=["bronze", "stocks"]
# tags=["bronze", "stocks"]

# Tags is a list — so you can add as many tags as you want
# One tag → ["bronze"]
# Two tags → ["bronze", "stocks"]
# Three tags → ["bronze", "stocks", "daily"]

# In Airflow UI it helps you filter and search DAGs:
# Filter by tag: "bronze" → shows all bronze layer DAGs
# Filter by tag: "stocks" → shows all stock related DAGs
# So one DAG can belong to multiple categories at once — that's why it's a list!

def minio_to_snowflake_dag():

    @task
    def get_unprocessed_files(**kwargs):
        """Only fetch files not yet processed — avoids duplicates"""
        s3 = get_minio_client()   
        bucket = "bronze-transactions"

        # get all files in bucket
        response = s3.list_objects_v2(Bucket=bucket)
        all_files = [obj["Key"] for obj in response.get("Contents", [])]


        #all_files = [obj["Key"] for obj in response.get("Contents", [])]
        #Let me break this into pieces:
        
        # Step 1 — response.get("Contents", []):

        # Gets the list of file objects from the response dict
        # # If "Contents" doesn't exist (empty bucket) → returns []
        # [
        #     {"Key": "AAPL/123.json", "Size": 245},
        #     {"Key": "GOOG/456.json", "Size": 312},
        #     {"Key": "MSFT/789.json", "Size": 198},
        # ]
        
        # Step 2 — for obj in ...: iterate all data one by on of response.get("Contents", [])]
        # Each obj is one file's dictionary:  obj = {"Key": "AAPL/123.json", "Size": 245}

        # Step 3 — obj["Key"]: #since only "key" is mentioned, it will only take info from "key" section.
        # Pulls just the file path from each obj
        # "AAPL/123.json"
        
        # Full result:
        # all_files = ["AAPL/123.json", "GOOG/456.json", "MSFT/789.json"]

        # Written the long way it would be:
        # all_files = []
        # for obj in response.get("Contents", []):
        #     all_files.append(obj["Key"])
        # List comprehension is just the short version of this!


        # get already processed files (stored in a tracking file in MinIO)
        try:
            processed_obj = s3.get_object(Bucket=bucket, Key="_processed/processed_keys.json")
            processed_files = json.loads(processed_obj["Body"].read())
        except Exception:
            processed_files = []  # first run — nothing processed yet

#         Think of _processed/processed_keys.json as a diary file stored in MinIO that keeps track of what's already been processed:
# json["AAPL/123.json", "GOOG/456.json"]

# s3.get_object() → reads that diary file from MinIO
# processed_obj["Body"].read() → reads the actual content of the file as bytes
# json.loads() → converts it to a Python list

# Why try/except?
# First ever run → diary file doesn't exist yet
#                 → s3.get_object() crashes
#                 → except catches it
#                 → processed_files = []  (start with empty list, that's fine)

# Second run onwards → diary file exists
#                    → reads it successfully
#                    → processed_files = ["AAPL/123.json", ...]


        
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
                # AUTO_COMPRESS=TRUE Snowflake automatically compresses files while uploading — faster
                logger.info(f"Staged file: {f}")

            cur.execute("""
                COPY INTO bronze_stock_quotes_raw
                FROM @%bronze_stock_quotes_raw
                FILE_FORMAT = (TYPE=JSON)
                ON_ERROR = 'CONTINUE' #if one file has bad data, skip it and continue instead of stopping everything
            """)
            logger.info("COPY INTO Snowflake executed successfully")

        except Exception as e:
            logger.error(f"Snowflake load failed: {e}")
            raise
        finally: #finally ensures connections are always closed — even if something crashes halfway through.
            cur.close()   # always close — even if error occurs
            conn.close()
# try:
#     # do something
# except:
#     # handle error
# finally:
#     # THIS ALWAYS RUNS — even if error occurred
#     cur.close()
#     conn.close()


    
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

# Run 1:
#   diary doesn't exist → processed_files = []
#   extend with ["AAPL/123.json", "GOOG/456.json"]
#   save diary = ["AAPL/123.json", "GOOG/456.json"]

# Run 2:
#   read diary → processed_files = ["AAPL/123.json", "GOOG/456.json"]
#   extend with ["MSFT/789.json"]
#   save diary = ["AAPL/123.json", "GOOG/456.json", "MSFT/789.json"]

# Run 3:
#   read diary → all 3 files already there
#   no new files → nothing to extend
#   same diary saved


    
    # Task dependencies
    new_files = get_unprocessed_files()
    downloaded = download_files(new_files)
    load_to_snowflake(downloaded)
    mark_files_processed(new_files)

minio_to_snowflake_dag()




#"Library returns an Object → Object carries methods → Variable holds that Object → Variable can use those methods"



