from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from google.cloud import storage
import json

# Use the environment variable for the MongoDB URI
mongo_uri = os.getenv('MONGO_URI')  # Get the MongoDB URI from environment variable
client = MongoClient(mongo_uri, serverSelectionTimeoutMS=50000)
db = client['mydatabase']
collection = db['summary']

def get_product_name(url):
    """Fetch product name from the given URL."""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            product_name = soup.find('h1', class_='page-title')
            if product_name:
                return product_name.text.strip()
        return None
    except Exception as e:
        print(f"Error fetching product name for URL {url}: {e}")
        return None

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

try:
    product_urls = []
    product_ids = []

    # Query to filter documents
    query = {
        'collection': {
            '$in': ['view_product_detail', 'select_product_option', 'select_product_option_quality', 'product_detail_recommendation_visible']
        }
    }

    # Fetch data from MongoDB
    data = collection.find(query)

    # Extract relevant fields
    for document in data:
        if 'current_url' in document:
            product_urls.append(document['current_url'])
        
        if document.get('collection') == 'product_detail_recommendation_visible':
            if 'viewing_product_id' in document:
                product_ids.append(document['viewing_product_id'])
        else:
            if 'product_id' in document:
                product_ids.append(document['product_id'])

    # Create a DataFrame
    df = pd.DataFrame({'Product_URL': product_urls, 'Product_ID': product_ids})

    # Fetch product names
    df['Product_Name'] = df['Product_URL'].apply(get_product_name)

    # Drop rows with missing product names
    df = df.dropna(subset=['Product_Name'])

    # Save to NDJSON file
    ndjson_file = "products.ndjson"
    with open(ndjson_file, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            json.dump({'name': row['Product_Name'], 'id': row['Product_ID']}, f, ensure_ascii=False)
            f.write("\n")

    print(f"NDJSON file saved: {ndjson_file}")

    # Upload to GCS
    bucket_name = "glamira-prj1"
    destination_blob_name = "json_product_name_ndjson.ndjson"
    upload_to_gcs(bucket_name, ndjson_file, destination_blob_name)

except ServerSelectionTimeoutError as e:
    print(f"Error connecting to MongoDB: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
