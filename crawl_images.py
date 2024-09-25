import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import random


df = pd.read_csv("C:/Users/ACER/OneDrive/Documents/data.csv")


if 'Unnamed: 0' in df.columns:
    df = df.drop('Unnamed: 0', axis=1)


session = requests.Session()
retry_strategy = requests.packages.urllib3.util.retry.Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    backoff_factor=random.uniform(0, 10)
)
adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_image_sources(url):
    try:
        response = session.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    img_tags = soup.find_all('img')
    img_sources = [img['src'] for img in img_tags if 'src' in img.attrs]
    
    return img_sources

def download_image(url, folder_path):
    try:
        response = session.get(url, stream=True)
        response.raise_for_status()
        
        
        filename = url.split("/")[-1]
        file_path = os.path.join(folder_path, filename)
        
       
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
        print(f"Downloaded {url} to {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")


output_folder = "C:/Users/ACER/Downloads/product_images"
os.makedirs(output_folder, exist_ok=True)


all_image_sources = set()  

for url in df['Product_URL']:
    image_sources = fetch_image_sources(url)
    all_image_sources.update(image_sources)

print(f"Total unique images found: {len(all_image_sources)}")


for image_url in all_image_sources:
   download_image(image_url, output_folder)
