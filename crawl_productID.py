from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import pandas as pd
import os  # Import os module to access environment variables

# Use the environment variable for the MongoDB URI
mongo_uri = os.getenv('MONGO_URI')  # Get the MongoDB URI from environment variable
client = MongoClient(mongo_uri, serverSelectionTimeoutMS=50000)
db = client['mydatabase']
collection = db['summary']

try:
    product_urls = []
    product_ids = []
    
    query = {
        'collection': {
            '$in': ['view_product_detail', 'select_product_option', 'select_product_option_quality', 'product_detail_recommendation_visible']
        }
    }

    data = collection.find(query)

    for document in data:
        if 'current_url' in document:
            product_urls.append(document['current_url'])
        
        if document.get('collection') == 'product_detail_recommendation_visible':
            if 'viewing_product_id' in document:
                product_ids.append(document['viewing_product_id'])
        else:
            if 'product_id' in document:
                product_ids.append(document['product_id'])
    
    print("Product URLs:", product_urls)
    print("Product IDs:", product_ids)

except ServerSelectionTimeoutError as e:
    print(f"Error: {e}")
