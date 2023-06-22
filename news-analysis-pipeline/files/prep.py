
from azure.storage.blob import BlobServiceClient

from transformers import AutoTokenizer, AutoModelForSequenceClassification

import argparse

def main():

    parser = argparse.ArgumentParser("prep")
    parser.add_argument("--prep_data", type=str, help="Path of prepped data")
    args = parser.parse_args()

    # log in to the Blob Service Client
    account_url = "https://mlstorageleo.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url, account_key=constants.BLOB_KEY)

    # connect to the container 
    container_client = blob_service_client.get_container_client(container="stock-news-json") 

    # list and download all currently available blobs
    blob_list = container_client.list_blobs()

    # get the timestamp with the current day 
    current_day_timestamp = datetime.datetime.today().timestamp()
    current_day_timestamp = str(current_day_timestamp)[:8] # first 8 digits are the timestamp of the day

    blobs_to_download = [blob.name for blob in blob_list if current_day_timestamp in blob.name]
    for blob in blobs_to_download:
        download_file_path = os.path.join(args.prep_data, str(blob))
        with open(file=download_file_path, mode="wb") as download_file:
            download_file.write(container_client.download_blob(blob).readall())

    # download distilbert model from HuggingFace
    tokenizer = AutoTokenizer.from_pretrained("KernAI/stock-news-destilbert")
    model = AutoModelForSequenceClassification.from_pretrained("KernAI/stock-news-destilbert")

if __name__ == "__main__":
    main()
