import requests
import json
from dotenv import load_dotenv
from os import getenv
from pymongo import MongoClient
from datetime import datetime
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer

# Load environment variables
load_dotenv()
API_TOKEN = getenv("G2_API_KEY")
MONGO_CONN_STRING = getenv("MONGO_CONN_STRING")
GOOGLE_TOKEN = getenv("GOOGLE_API_KEY")

# Configure Google Generative AI
genai.configure(api_key=GOOGLE_TOKEN)

# MongoDB setup
mongo_client = MongoClient(MONGO_CONN_STRING)
db = mongo_client.g2hack
products_collection = db.unavailableProducts
unprocessed_products_collection = db.unprocessedCollection

def check_mongo_connection():
    """ Checks MongoDB connection. """
    try:
        mongo_client.server_info()
        print("Connected to MongoDB")
    except Exception as e:
        print("Failed to connect to MongoDB:", e)


def fetch_products_from_google(message_data):
    """ Fetches product information using Google's generative AI. """
    model = genai.GenerativeModel('gemini-pro')
    prompt = f"""{message_data} \n Imagine a digital assistant meticulously analyzing a diverse collection of announcements related to the launch 
    of new products and services in various industries. This assistant is tasked with identifying and categorizing each product or service mentioned,
      discerning whether each one represents a fresh market entry or an update to an existing offering. The goal is to compile this information into a
        straightforward, accessible format. Specifically, the assistant is required to present its findings as a list, focusing solely on the names of
          these products or services, neatly organized into an array. The array should exclusively contain the names, clearly distinguishing between
            novel introductions and updates to pre-existing entities, thus providing a clear, concise overview of the recent developments highlighted
              in the announcements, identify if it a B2B product. 
              Determine from the following news excerpt whether it pertains to a B2B software product and if the product mentioned is new or already existing. Provide a one-word answer for each question.
                Give the output in a json format which gives the product name and the status of the same whether its a new product
                or just a update to the existing product. The status should either be New Product or Update to existing product.Keep the key name of the
                  product name as Product Name and the status as Status """
    response = model.generate_content(prompt)
    return response.text

def process_product_info(message_data):
    """ Processes each message to extract and handle product information. """
    response_text = fetch_products_from_google(message_data)
    if response_text:
        clean_json = response_text.lstrip("```json").lstrip("```JSON").rstrip("```").strip()
        products = json.loads(clean_json)
        print(products)
            
               
        

def process_individual_product(product):
    """ Processes each individual product entry. """
    status = product.get("Status")
    product_name = product.get('Product Name')
    if status == "New Product":
        print(f"Processing new product: {product_name}")
        # handle_new_product(product_name)
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

def main():
    """ Main function to setup Kafka consumer and process messages in 10-second batches. """
    # MongoDB connection and Kafka configuration
    mongo_client = MongoClient(MONGO_CONN_STRING)
    db = mongo_client.g2hack
    unprocessed_products_collection = db.unprocessedCollection

    consumer = KafkaConsumer(
        'news',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my_group',
    )

    message_accumulator = []
    start_time = time.time()
    time_window = 10  # Set the time window for batch processing

    try:
        for message in consumer:
            decoded_message = message.value.decode('utf-8')
            message_accumulator.append(decoded_message)

            # Check if the time window has elapsed
            if time.time() - start_time >= time_window:
                # Process messages in bulk using a thread pool
                with ThreadPoolExecutor(max_workers=1) as executor:
                    for msg in message_accumulator:
                        executor.submit(process_product_info, msg)  # Process each message
                        preprocessed = msg.replace("\n", " ")
                        unprocessed_products_collection.insert_one({"feed": preprocessed, "timestamp": datetime.now()})

                message_accumulator = []  # Clear the message accumulator for the next batch
                start_time = time.time()  # Reset the start time for the new batch

    except Exception as e:
        print("Error during message processing:", e)

if __name__ == "__main__":
    main()


