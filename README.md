# [GLAMIRA BEHAVIOUR](www.glamira.com) DATA PIPELINE

 ![image](https://scamminder.com/include/uploads/2024/06/glamira.pt.png)
<!-- Start Document Outline -->
* Batch data pipeline
  * [:briefcase: Business Case and Requirement](#briefcase-business-case-and-requirement)
  * [:bookmark_tabs:Step by step this project](#step-by-step)
    * [Architecture](#architecture)
      * [ETL flow](#etl-flow)
    * [How to run](#how-to-run)
      * [Data collection](#data-collection)
      * [Data ingestion](#data-ingestion)
      * [Data transform](#data-transform)

---
## :briefcase: Business Case and Requirement
### 🔳Business case
In this project, we use raw data(behaviour user) in [www.glamira.com](www.glamira.com) transform into a more accessible format for extracting insights. 
### Presiquites
In this project, we'll use dbt (Data Build Tool) and SQL on Google BigQuery for data transformation. dbt, an open-source tool, will help us effectively transform data in our warehouses. We'll use SQL for data management and Google BigQuery, a fully-managed, serverless data warehouse, for super-fast SQL queries.

## :bookmark_tabs:Step by step this project
### Architecture
The pipeline take data from [www.glamira.com](www.glamira.com) and transform into insight data
 - **Google cloud platform**: Cloud storage(load raw data),bigquery (load schema data and transform shema data), cloud function (trigger data in Cloud storage into bigquery), 
 - **Looker studio**: visualize data
 ![471869237_896236892721838_9127615373039329563_n](https://github.com/user-attachments/assets/e1f680d8-bb07-46a3-82e8-b474c3906b33)
# ETL flow
- You run a code in `product_name_soup.ipynb` and `IP_glamira-python.py` to crawl all properties relate to data(product name and ip detail)
- Raw data , product name, ip location save in Google Cloud Storage
    - raw data 
    - product name
    - ip location
  - create cloud function to trigger from cloud storage to bigquery use `schema`
  - use `dbt` to transform raw data to datawarehouse
  - use Looker Studio to visualize
### How to run
#### Data Profiling
The data profiling steps are designed to analyze and assess the quality of the data in the `SeminarCollection` collection, including:
- Identify columns and data types: Determine the field names and data types in the collection to ensure consistency and data integrity.
- Count null values: Check and count the number of `null` values in each field to assess the completeness of the data.
- Count distinct values: Detect and remove duplicate rows by counting distinct values in key fields like `time_stamp`, `ip`, `user_agent`, etc.
- Check data type consistency: Ensure that fields such as `price` have valid data types and formats (e.g., decimal format).
- Data completeness: Count the total number of documents in the collection to evaluate the completeness and fullness of the data.
- Determine data range: Check the time range of the collected data from the first to the last timestamp to understand the span of the dataset.
- Currency validation: Count and sort the different `currency` values in the dataset to identify the variety and distribution of currencies.

#### Data collection
- As data lack of information, so we need to crawl more inforamtion in Glamira web, especially **product name**.(Crawl product name in file `crawl_data`)
- Suplement data with extending ip(use file `IPLocation-Python-master`) 
- push data to **mongodb** and config **connect_db**
```python
from pymongo import MongoClient

def connect_db():
    """Kết nối đến cơ sở dữ liệu MongoDB."""
    client = MongoClient("your local host")
    db = client['database']
    collection = db['collection']
    return collection
```
#### Data ingestion
- Put all data into data lake(Cloud Storage)
- Trigger data from data lake to data warehouse(Cloud Storage to Bigquery)
  - We need to transform data to jsonl format.(file in `transform_jsonl`)
  - Use Cloud Function to trigger from Cloud Storage to Bigquery(file in `schema`)

#### Data transform
Use SQL and dbt to transform raw data to Dimensional model in Bigquery. Save in layer glamira_transform. Please survey your dataset first and Filter collection key = "checkout_success" for fact table(file in `dbt`)
This is schema of data warehouse in my project ![471869237_896236892721838_9127615373039329563_n](https://github.com/user-attachments/assets/df03028d-f428-4d43-9fc6-c3d49fc25ee4)

### Data visualization
- [ ]  Import data model into Looker and Visualize data to answer below questions
- Which products (product_name) generate the most revenue?
- How do the total sales (line_total) trend over different days?
- How are sales distributed across different countries (country_name)?
- How are revenue distribute with alloy and diamond?
You can reference my report [Looker Studio](https://lookerstudio.google.com/u/0/reporting/c0fd6bca-1ced-466a-a3d9-9a957b56fb35/page/8FuaE?fbclid=IwY2xjawHl3GtleHRuA2FlbQIxMAABHee1xGZTJ-3GSaxJ3bnWldd5xnDMDdo5L6R45fvltYHweEHrZGUjjrHklQ_aem_R8lb1IcNSiMTET7uk-3E2w)

### Recommendation System
- [x] Built recommendation system to enhance personalized user experience.
- 📌 **Techniques Used**:
  - **Item-to-User Collaborative Filtering**: Suggest products to users based on similar users' behavior.
  - **Item-to-Item Similarity**: Recommend similar products based on product co-occurrence or similarity metrics.
- ⚙️ **Data Processing**: User-product interaction matrix was built using purchase history and product attributes.
- 🚀 **Demo App**:
  - Developed an interactive app using **Streamlit** to allow users to test and explore recommendations.
  - Features include:
    - Entering a user email to get personalized recommendations.
    - Choosing a product to get similar product recommendations.

<img width="1681" height="666" alt="518157808_1430774288164886_600609317733105453_n" src="https://github.com/user-attachments/assets/a2cbcc7c-e175-4053-839c-882f72dde1e2" />
<img width="1628" height="519" alt="519949389_1294922161998010_8718763120915204678_n" src="https://github.com/user-attachments/assets/bcc46b49-bb58-405d-aef7-360fefdc130a" />
<img width="1622" height="829" alt="518824061_1079721106922451_9134499011795668295_n" src="https://github.com/user-attachments/assets/4625f6da-e3e7-411f-873a-06427b751329" />
<img width="1596" height="613" alt="518769209_2664062787269472_7686603722646551876_n" src="https://github.com/user-attachments/assets/ec8a756f-2045-4d33-b74d-d4911165941e" /> 
<img width="1881" height="843" alt="518252451_1919728565470434_2171709965157296504_n" src="https://github.com/user-attachments/assets/e12a8c62-3310-43b2-905f-6f74fe112a0e" />







