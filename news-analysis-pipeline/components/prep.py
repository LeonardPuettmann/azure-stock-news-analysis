
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pathlib import Path
import datetime
import json

credential = DefaultAzureCredential()
# Check if given credential can get token successfully.
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

import argparse

parser = argparse.ArgumentParser("prep")
parser.add_argument("--blob_storage", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--prep_output")
args = parser.parse_args()

# log in to the Blob Service Client
blob_storage = args.blob_storage
blob_storage_key = secret_client.get_secret("blob-storage-key")
blob_service_client = BlobServiceClient(blob_storage, account_key=blob_storage_key.value)

# connect to the container 
container_client = blob_service_client.get_container_client(container="stock-news-json") 

# list and download all currently available blobs
blob_list = container_client.list_blobs()
print(f"Blob from: {blob_storage} has these blobs today: {blob_list}")

# get the timestamp with the current day 
current_day_date = datetime.datetime.today().isoformat()[:10]

# filter out which blobs have the current date and download them
blobs_to_use = [blob.name for blob in blob_list if current_day_date in blob.name]
for blob in blobs_to_use:
      print(f"Downloading blob: {blob}")
      blob_client = blob_service_client.get_blob_client(container="stock-news-json", blob=blob)
      with open(blob, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

# combine all blobs into one dictionary
all_data_dict = {}
for json_file in blobs_to_use:
      with open(json_file,"r+") as file:
      # First we load existing data into a dict.
            file_data = json.load(file)
            all_data_dict.update(file_data)

# pass aggregated file to the next step        
with open((Path(args.prep_output) / "merged_stock_news.json"), "w") as file:
      file.write(json.dumps(all_data_dict, indent=4))
