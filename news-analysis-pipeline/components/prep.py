
from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from pathlib import Path
import datetime
import json

credential = ManagedIdentityCredential(client_id="a4d3af98-d651-42db-850f-485cbe770757")
secret_client = SecretClient(vault_url="https://mlgroupvault.vault.azure.net/", credential=credential)

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
current_day_timestamp = datetime.datetime.today().timestamp()
current_day_timestamp = str(current_day_timestamp)[:5] # first 8 digits are the timestamp of the day

blobs_to_use = [blob.name for blob in blob_list if current_day_timestamp in blob.name]
for blob in blobs_to_use:
      print(f"Downloading blob: {blob}")
      blob_client = blob_service_client.get_blob_client(container="stock-news-json", blob=blob)
      with open(blob, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

all_data_dict = {}
for json_file in blobs_to_use:
      with open(json_file,"r+") as file:
      # First we load existing data into a dict.
            file_data = json.load(file)
            all_data_dict.update(file_data)
with open("merged_stock_news.json", "w") as file:
      file.write(json.dumps(all_data_dict, indent=4))

# with open(args.blob_storage+"/blobs_to_use.txt", "w") as f:
#     f.write("\n".join(blob for blob in blobs_to_use), f)

(Path(args.prep_output) / "merged_stock_news.json")#.write_text("\n".join(blob for blob in blobs_to_use))
# continue here with this example -> https://github.com/Azure/azureml-examples/blob/main/sdk/python/jobs/pipelines/1a_pipeline_with_components_from_yaml/score_src/score.py
