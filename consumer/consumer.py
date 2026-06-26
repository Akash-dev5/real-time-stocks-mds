#Import requirements
import json
import boto3
import time
from kafka import KafkaConsumer

#Minio Connection
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)
# Pattern: Library → Object → Methods
# boto3 → S3 Client Object
# s3 = boto3.client("s3", ...)

# boto3.client() returns an S3 Client Object
# Stored in variable s3
# That's why s3 can use:

# s3.head_bucket(...)    ✅ method of S3 Client Object
# s3.create_bucket(...)  ✅ method of S3 Client Object
# s3.put_object(...)     ✅ method of S3 Client Object



bucket_name = "bronze-transactions"

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")



#Define Consumer
consumer = KafkaConsumer(     #KafkaConsumer() returns a Kafka Consumer Object Stored in variable consumer That's why consumer can be looped over: for message in consumer:  ✅ consumer object is iterable
    "stock-quotes",
    bootstrap_servers=["host.docker.internal:29092"],
    auto_offset_reset="earliest", #Suppose Kafka already contains (msg1, msg2, msg3) If this consumer is brand new (starts rading from msg1)
    
    #auto_offset_reset="latest" then it will ignore old msgs (only reads new ones).
    
    enable_auto_commit=True,    #Kafka remembers "I already processed Message 100", So if consumer restarts, it resumes from Message 101.
    
    group_id="bronze-consumer1", #This identifies the consumer group. Multiple consumers with the same group ID can share the work.
    
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) #json.dumps(v) = (is a function call) while, v.decode("utf-8") = (is a method call on an object.)
)

print("Consumer streaming and saving to MinIO...")



#Main Function
for message in consumer: #it's iterating through Kafka messages.
    record = message.value
    symbol = record.get("symbol", "unknown")   #.get() is not a Kafka method — it's a built-in Python dictionary method
    ts = record.get("fetched_at",int(time.time()))
    key = f"{symbol}/{ts}.json"

    # message → Kafka Message Object
    # for message in consumer:
    # record = message.value

    # Each iteration gives you a Kafka Message Object
    # Stored in variable message
    # That's why message can use:

    # message.value      ✅ attribute of Kafka Message Object
    # message.key        ✅
    # message.offset     ✅

    # KafkaConsumer
    # │
    # └── gives → message  (Kafka Message Object)
    #                 │
    #                 └── .value  gives → record  (Plain Python Dictionary)
    #                                         │
    #                                         └── .get()  ← built-in DICT method
    #                                                        NOT from Kafka


    
    s3.put_object(
        Bucket=bucket_name, #Which S3 bucket to save into
        Key=key,            #The file path/name inside the bucket
        Body=json.dumps(record),   #The actual content to save — json.dumps(record) converts the dict to a JSON string
        ContentType="application/json"  #Tells S3 this is a JSON file
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")



# Producer
#   │  Python Dict → json.dumps() → JSON String → .encode() → Bytes
#   │
#   │  (travels through Kafka as Bytes)
#   ▼
# Consumer
#   │  Bytes → .decode("utf-8") → JSON String → json.loads() → Python Dict
#   │
#   │  (you work with it as Python Dict)
#   ▼
# S3
#   │  Python Dict → json.dumps() → JSON String
#   │
#   └──  saved as .json file in S3   #Because S3 can't store a Python dictionary — it can only store text/bytes, so you convert it back to a JSON string before saving.


# Whenever a library function/method returns an object → that object carries its own methods with it → your variable inherits all those methods just by holding that object.
# This is the concept of Object Oriented Programming (OOP)
                    
