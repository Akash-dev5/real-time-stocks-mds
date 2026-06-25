#Import requirements
import time 
import json
import requests
from kafka import KafkaProducer

#Define variables for API
API_KEY="<<YOUR API KEY>>"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"] #ticker symbols

#Initial Producer
producer = KafkaProducer (
    bootstrap_servers=["host.docker.internal:29092"], #HOST_PORT : CONTAINER_PORT
    #Lambda version
    value_serializer=lambda v: json.dumps(v).encode("utf-8") #it converts python dictoinary -> JSON string -> bytes (because Kafka cannot directly send Python dictionaries.)
    #lambda is just a short way to write a function.  (lambda parameters: expression)
    #lambda is basically a way to create a small anonymous function (a function without a name).
    
    #Normal function version
    # def serializer(v):
    # return json.dumps(v).encode("utf-8")

    # producer = KafkaProducer(
    #     value_serializer=serializer
    #)

    # quote dictionary created
    #     ↓
    # producer.send(value=quote)
    #     ↓
    # Kafka calls your serializer
    #     ↓
    # serializer(quote)
    #     ↓
    # v = quote
    #     ↓
    # json.dumps(v)
    #     ↓
    # encode("utf-8")
    #     ↓
    # Kafka sends bytes
)

#Retrive Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:  #"Try running this code. If something fails, jump to the except block."
        response = requests.get(url) #This sends an HTTP GET request.
        response.raise_for_status() #Suppose API returns:(200 ok) No problem, if API returns (401 Unauthorized) or (404 Not Found), Then Python raises an exception and jumps to:(except Exception as e:)
        data = response.json() #it converts JSON string to python dictionary because Adding fields to a dictionary is easy.
        
        # {
        # "c": 201.45,
        # "h": 205.00,
        # "l": 198.00
        # }
        
        data["symbol"] = symbol                 #---|
        data["fetched_at"] = int (time.time())  #---|-- these 2 lines has been added in the data 
        
        # {
        # "c": 201.45,
        # "h": 205.00,
        # "l": 198.00,
        # "symbol": "AAPL",             #data["symbol"] = symbol
        # "fetched_at": 1719234567      #data["fetched_at"] = int (time.time())
        # }
        
        return data  #Returns the dictionary to the caller.
    except Exception as e:   # If anything fails: (Internet issue, API down, Invalid API key, Timeout) Python jumps here.
        print(f"Error fetching {symbol}: {e}") #Error fetching AAPL: 404 Client Error
        return None

#Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}") #
            producer.send("stock-quotes", value=quote) #then Kafka automatically runs:(json.dumps(quote).encode("utf-8")) before sending and data become bytes (b'{"symbol":"AAPL","price":200}') and gets stored in Kafka.
    time.sleep(6)   #API allows 60 requests per minute.




# ports: "9999:5432"

# it means:
# Your PC (localhost:9999)
#           ↓
# PostgreSQL Container (5432)

# So in DBeaver you would enter:
# Host: localhost
# Port: 9999
# Database: postgres (or your database name)
# Username: postgres
# Password: ****

# DBeaver sends traffic to: localhost:9999

# Docker receives it and forwards it to: PostgreSQL Container:5432
#where PostgreSQL is listening.
