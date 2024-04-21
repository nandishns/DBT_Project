from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import requests
import json
from dotenv import load_dotenv
from os import getenv
from pymongo import MongoClient
from datetime import datetime
import time

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
        document = {
                "product_name": product_name,
                "timestamp": datetime.now(),
                "desc": message_data.get('description'),
            }
        unavailable_products_collection.insert_one(document)
def main():
    ping_mongo()  # Check MongoDB connection
    print("Setting up Kafka consumer")
    consumer = KafkaConsumer(
        'Software',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    message_accumulator = []
    start_time = time.time()
    time_window = 10  # Time window set to 10 seconds

    try:
        for message in consumer:
            message_accumulator.append(message.value)

            # Check if the time window has elapsed
            if time.time() - start_time >= time_window:
                # Process all accumulated messages concurrently
                with ThreadPoolExecutor(max_workers=10) as executor:
                    for msg in message_accumulator:
                        executor.submit(process_message, msg)

                # Reset the accumulator and the timer
                message_accumulator = []
                start_time = time.time()

    except Exception as e:
        print("Error during message processing:", e)

if __name__ == "__main__":
    main()