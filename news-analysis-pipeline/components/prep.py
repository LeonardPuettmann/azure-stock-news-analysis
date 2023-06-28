
from azure.storage.blob import BlobServiceClient

import argparse

def main():

    parser = argparse.ArgumentParser("prep")
    parser.add_argument("--blob_storage_read", type=str, help="Mounted Azure ML blob storage")
    parser.add_argument("--account_url")
    args = parser.parse_args()

    # log in to the Blob Service Client
    account_url = args.account_url
    blob_service_client = BlobServiceClient(account_url, account_key=constants.BLOB_KEY)

    # connect to the container 
    container_client = blob_service_client.get_container_client(container="stock-news-json") 

    # list and download all currently available blobs
    blob_list = container_client.list_blobs()

    # get the timestamp with the current day 
    current_day_timestamp = datetime.datetime.today().timestamp()
    current_day_timestamp = str(current_day_timestamp)[:8] # first 8 digits are the timestamp of the day

    blobs_to_use = [blob.name for blob in blob_list if current_day_timestamp in blob.name]

    # ! the files should not be downloaded in this step. Instead it might make more sense to pass a list with the filenames to the next component
    with open(args.blob_storage+"/blobs_to_use.txt", "w") as f:
        f.write("\n".join(blob for blob in blobs_to_use), f)

if __name__ == "__main__":
    main()
