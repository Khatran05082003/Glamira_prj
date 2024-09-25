import pandas as pd
import requests
from bs4 import BeautifulSoup

df = pd.read_csv("C:/Users/ACER/Downloads/data.csv")

if 'Unnamed: 0' in df.columns:
    df = df.drop('Unnamed: 0', axis=1)

def get_product_name(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            product_name = soup.find('h1', class_='page-title').text.strip()
            return product_name
        else:
            return None 
    except Exception as e:
        return None  


df['Product_Name'] = df['Product_URL'].apply(get_product_name)
df = df.dropna()
df.to_csv("C:/Users/ACER/Downloads/data_1.csv", index=False)