import requests
import json
from dotenv import load_dotenv
from os import getenv
from pymongo import MongoClient
from datetime import datetime
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
import time

# Load environment variables
load_dotenv()
# API_TOKEN = getenv("G2_API_KEY")
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
    print("Fetching products from Google")
    model = genai.GenerativeModel('gemini-pro')
    prompt = f"""{message_data} \n Imagine a digital assistant meticulously analyzing a diverse collection
      of announcements related to the launch of new products and services in various industries.
        This assistant is tasked with identifying and categorizing each product or service mentioned, 
        discerning whether each one represents a fresh market entry or an update to an existing offering.
          The goal is to compile this information into a straightforward, accessible format. Specifically,
            the assistant is required to present its findings as a list, focusing solely on the names of these 
            products or services, neatly organized into an array. The array should exclusively contain the names, 
            clearly distinguishing between novel introductions and updates to pre-existing entities, thus providing a clear,
              concise overview of the recent developments highlighted in the announcements, identify if the product is B2B product. 
              Make sure that the product you identify is a B2B product and only then include it in the list.
                Give the output in a json format which gives the product name and the status of the same whether
                  its a new product or just a update to the existing product. The status should either be New Product 
                  or Update to existing product.Keep the key name of the product name 
                  as Product Name and the status as Status """
    response = model.generate_content(prompt)
    # print(response.text)
    return response.text

def process_product_info(message_data):
    """ Processes each message to extract and handle product information. """
    print("Processing message")
    response_text = fetch_products_from_google(message_data)
    if response_text:
        clean_json = response_text.lstrip("```json").lstrip("```JSON").rstrip("```").strip()
        print(clean_json, "JSON Response")
        try:
            products = json.loads(clean_json)
            print("Products:", products)
            # for product in products:
            #     process_individual_product(product)
        except json.JSONDecodeError as e:
            # print(f"Failed to decode JSON: {str(e)}")
            pass

def process_individual_product(product):
    """ Processes each individual product entry. """
    status = product.get("Status")
    product_name = product.get('Product Name')
    if status == "New Product":
        print(f"Processing new product: {product_name}")
        document = {
                "product_name": product_name,
                "timestamp": datetime.now(),
            }
        products_collection.insert_one(document)


def main():
    """ Main function to setup Kafka consumer and process messages. """
    check_mongo_connection()
    print("Setting up Kafka consumer")
    consumer = KafkaConsumer(
        'x-llm',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
    )

    message_accumulator = []
    start_time = time.time()
    time_window = 10  # Process data every 10 seconds

    try:
        for message in consumer:
            decoded_message = message.value.decode('utf-8')
            message_accumulator.append(decoded_message)
            current_time = time.time()
            
            # Check if 10 seconds have elapsed
            if current_time - start_time >= time_window:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    for msg in message_accumulator:
                        executor.submit(process_product_info, msg)
                        preprocessed = msg.replace("\n", " ")
                        unprocessed_products_collection.insert_one({"feed": preprocessed, "timestamp": datetime.now()})
                
                # Reset the accumulator and timer
                message_accumulator = []
                start_time = time.time()

    except Exception as e:
        print("Error during message processing:", e)

if __name__ == "__main__":
    main()
