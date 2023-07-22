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
current_day_date = datetime.datetime.today().isoformat()[:10]

blob_to_use = [blob.name for blob in blob_list if current_day_date in blob.name][0]
print(f"Downloading blob: {blob_to_use}")
blob_client = blob_service_client.get_blob_client(container="stock-news-json", blob=blob_to_use)
with open(blob_to_use, mode="wb") as sample_blob:
    download_stream = blob_client.download_blob()
    sample_blob.write(download_stream.readall())

with open(blob_to_use,"r+") as file:
    data = json.load(file)
            

