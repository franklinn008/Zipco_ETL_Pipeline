#Importing libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from Transformation import run_transformation
from Extraction import run_extraction

def run_loading():
    #Data loading
    #Setting up Azure blob connection using .env file
    load_dotenv()
    connect_str = os.getenv('CONNECT_STR')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_name = os.getenv('CONTAINER_NAME')
    container_client = blob_service_client.get_container_client(container_name)

    #Saving dataset/tables as csv
    data = pd.read_csv('newcleaned.csv')
    products= pd.read_csv('products.csv')
    customers= pd.read_csv('customers.csv')
    staff= pd.read_csv('staff.csv')
    transactions= pd.read_csv('transactions.csv')

    #Loading files in tuples
    files = [
        (data, 'rawdata/cleaned_main.csv'),
        (products, 'clean_data/products.csv'),
        (customers, 'clean_data/customers.csv'),
        (staff, 'clean_data/staff.csv'),
        (transactions, 'clean_data/transactions.csv'),
            
        ]

    for file,blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite = True)
        print(f'{blob_name} uploaded to  Blob Storage succesfully')

    