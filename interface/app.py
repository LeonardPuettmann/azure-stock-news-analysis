import streamlit as st 

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import datetime
import json

credential = DefaultAzureCredential()
# Check if given credential can get token successfully.
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

# log in to the Blob Service Client
blob_storage_key = secret_client.get_secret("blob-storage-key")
blob_service_client = BlobServiceClient("https://mlstorageleo.blob.core.windows.net/", account_key=blob_storage_key.value)

# connect to the container 
container_client = blob_service_client.get_container_client(container="processed-stock-news-json") 

# list and download all currently available blobs
blob_list = container_client.list_blobs()
available_dates = [datetime.datetime.strptime(str(file_name.name)[21:31],"%Y-%m-%d") for file_name in blob_list] # processed-stock-news-2023-07-22.json
selected_date = st.sidebar.date_input("Start date", value=max(available_dates), min_value=min(available_dates), max_value=max(available_dates), format="YYYY-MM-DD")

tickers = ["MSFT", "AAPL", "DOCN", "AVGO", "TXN", "IBM"]
ticker_symbol = st.sidebar.selectbox('Stock ticker', tickers) 

# for blob in blob_list:
#     blob_client = blob_service_client.get_blob_client(container="stock-news-json", blob=blob)
#     with open(f"blobs/{blob}", mode="wb") as sample_blob:
#         download_stream = blob_client.download_blob()
#         sample_blob.write(download_stream.readall())

# with open(blob,"r+") as file:
#     data = json.load(file)


            

