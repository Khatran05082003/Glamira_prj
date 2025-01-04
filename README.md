# Batch Data Pipeline Project for Glamira

## 💼 Business Case and Requirements

### Project Overview
This project leverages raw user behavior data sourced from [www.glamira.com](https://www.glamira.com), transforming it into a more structured format that facilitates actionable insights.

![image](https://github.com/user-attachments/assets/b8b2fbaa-acff-45c9-a321-2f3d5feba489)


### Prerequisites
To execute this project, we will use dbt (Data Build Tool) and SQL within Google BigQuery for data transformation. Dbt, being an open-source solution, will streamline our data management and transformation processes. Meanwhile, Google BigQuery serves as a fully-managed, serverless data warehouse, providing rapid SQL query capabilities.

## 📑 Step-by-Step Guide

### System Architecture
The data pipeline is designed to extract and process data from www.glamira.com, transforming it into useful insights. The architectural components include:

- **Google Cloud Platform Components:**
  - **Cloud Storage:** For storing raw data.
  - **BigQuery:** For schema data loading and transformation.
  - **Cloud Function:** To automate data transfer from Cloud Storage to BigQuery.
  - **Looker Studio:** For visualizing the architecture and insights.

### ETL Workflow
1. Execute the scripts `product_name_soup.ipynb` and `IP_glamira-python.py` to collect all relevant properties, including product names and IP details.
2. Store the raw data, product names, and IP locations in Google Cloud Storage.
3. Establish a Cloud Function to initiate data transfer from Cloud Storage to BigQuery according to the defined schema.
4. Employ dbt to convert raw data into a structured data warehouse schema.
5. Utilize Looker Studio to visualize the transformed data.

### Execution Steps

#### Clone the Repository

To clone the repository, run the following command:
```bash
git clone https://github.com/yourusername/your-repository.git
```
#### Install Required Libraries
Make sure to install the necessary Python libraries listed in requirements.txt. You can do this by running:
```bash
pip install -r requirements.txt
```
#### File Descriptions

- **.vscode/**: Contains settings and configurations for Visual Studio Code to optimize your development environment.
- **.glamira_transform/**: This folder contains the dbt models and transformation scripts used to convert raw data into a structured format.
- **.logs/**: This directory is for storing log files generated during the execution of scripts and functions for tracking and debugging purposes.
- **.target/**: This folder is where dbt outputs its compiled models and generated artifacts after running the transformation.
- **Cloud Function.py**: Contains the code for the Google Cloud Function that triggers the data transfer from Cloud Storage to BigQuery.
- **crawl_images.py**: A script for crawling and collecting image data related to products on Glamira.
- **crawl_productID.py**: A script that retrieves product IDs from the Glamira website.
- **crawl_product_name.py**: A script designed to scrape product names from the Glamira site.
- **json_product_name_ndjson.ndjson**: The resulting NDJSON file that contains the crawled product names, ready for further processing.
- **requirements.txt**: Lists all the Python libraries required for the project, ensuring a smooth setup.


#### Data Collection
Given the incompleteness of the current dataset, additional data must be crawled from the Glamira website, with a focus on product names. Utilize the crawl_data file for this purpose, and enhance the dataset with extended IP information (refer to the IPLocation-Python-master).

#### Database Connection
After collecting the data, push it to MongoDB and set up the connection accordingly.

#### Data Ingestion
- **Data Storage**: All collected data is to be stored in the data lake (Google Cloud Storage).

- **Data Transfer**: Trigger the ingestion process from the data lake to the data warehouse (Cloud Storage to BigQuery).

- **Data Formatting**: Convert the collected data into JSONL format using the script provided in transform_jsonl.

- **Cloud Function Deployment**: Implement a Cloud Function to facilitate the transfer from Cloud Storage to BigQuery based on the specified schema.

- **Data Transformation**: Utilize SQL and dbt to reshape the raw data into a dimensional model in BigQuery. 

The output should be saved in the glamira_transform layer. Prior to transformation, ensure to thoroughly survey the dataset and filter for the collection key "checkout_success" to establish the fact table, as indicated in the dbt files.

#### Data Visualization
Import the finalized data model into Looker and generate visualizations to explore the following queries:

- **Revenue Insights**: Which products (product_name) are driving the highest revenue?

- **Sales Analysis**: What are the trends in total sales (line_total) over various months?

- **Sales Distribution**: How are sales dispersed across different countries (country_name)?


#### Conclusion
This project is designed to extract meaningful insights into user behavior and product performance at Glamira, thereby facilitating data-driven decision-making.

