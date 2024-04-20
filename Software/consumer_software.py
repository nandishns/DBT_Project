from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import requests
import json
from dotenv import load_dotenv
from os import getenv
from pymongo import MongoClient
from datetime import datetime


load_dotenv()
api_token = getenv("G2_API_KEY")
mongo_conn_string = getenv("MONGO_CONN_STRING")

# Setup MongoDB connection
client = MongoClient(mongo_conn_string)
db = client.g2hack
unavailable_products_collection = db.unavailableProducts

def ping_mongo():
    try:
        client.server_info()
        print("Connected to MongoDB")
    except:
        print("Failed to connect to MongoDB")



def process_message(message_data):
    product_name = message_data.get('name')
    desc = message_data.get('description')
    if product_name:
        print(f"Product Name: {product_name}, Description: {desc}")

def main():
    ping_mongo()
    print("Setting up Kafka consumer")
    consumer = KafkaConsumer(
        'Software',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    with ThreadPoolExecutor(max_workers=10) as executor:
        for message in consumer:
            data = message.value
            executor.submit(process_message, data)

if __name__ == "__main__":
    main()