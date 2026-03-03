import pandas as pd
from google.cloud import bigquery
import os

# Set your Windows path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\\Google API KEY\\google_key.json"

client = bigquery.Client()
project_id = "dbtbigquery-488818"  
dataset_id = "raw_olist"             # New dataset for this specific project

# 1. Create the Dataset in BigQuery
dataset_ref = client.dataset(dataset_id)
client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

# 2. The Exact Files from Your Screenshot
files_to_upload = [
    "customers.csv", 
    "geolocation.csv", 
    "order_items.csv", 
    "order_payments.csv", 
    "order_reviews.csv", 
    "orders.csv", 
    "product_category_name_translation.csv", 
    "products.csv", 
    "sellers.csv"
]

# 3. Loop and Upload
for file_name in files_to_upload:
    table_name = "raw_" + file_name.replace(".csv", "")
    file_path = os.path.join("../data/", file_name)
    
    print(f"Reading {file_name}...")
    # Using low_memory=False because geolocation.csv is large (~60MB)
    df = pd.read_csv(file_path, low_memory=False)
    
    # Clean column names just in case BigQuery complains
    df.columns = [str(c).replace(' ', '_').replace('-', '_') for c in df.columns]
    
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    print(f"Uploading to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Wait for the job to finish
    
    print(f"Success! Loaded {len(df)} rows into {table_name}\n")