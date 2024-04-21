from pymongo import MongoClient
from datetime import datetime, timedelta
from os import getenv
import google.generativeai as genai
import json
from dotenv import load_dotenv

load_dotenv()
MONGO_CONN_STRING = getenv("MONGO_CONN_STRING")
GOOGLE_TOKEN = getenv("GOOGLE_API_KEY")
print(GOOGLE_TOKEN)
mongo_client = MongoClient(MONGO_CONN_STRING)
db = mongo_client.g2hack
unprocessed_products_collection = db.unprocessedCollection
batchprocessed_products_collection = db.batchwiseProcessedCollection



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
            document={"productName" : products.get("Product Name"),"status":products.get("Status")}
            batchprocessed_products_collection.insert_one(document)
            print("product saved")
            # for product in products:
            #     process_individual_product(product)
        except json.JSONDecodeError as e:
            # print(f"Failed to decode JSON: {str(e)}")
            pass






def get_unprocessed_data(start_of_day,end_of_day):
    query = {
        "timestamp": {
            "$gte": start_of_day,
            "$lt": end_of_day
        }
    }
    pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$product_name",
        }},
        {"$sort": {"_id": 1}}  # Sort by product name
    ]
    results = unprocessed_products_collection.aggregate(pipeline)
    products = [{"feed": result.get("feed")} for result in results]
    return products

def batchwise_processing(products):
    for product in products:
        process_product_info(product)






def main():
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_day = today
    end_of_day = today + timedelta(days=1)
    check_mongo_connection()
    products = get_unprocessed_data(start_of_day,end_of_day)
    batchwise_processing(products)


if __name__ == "__main__":
    main()
