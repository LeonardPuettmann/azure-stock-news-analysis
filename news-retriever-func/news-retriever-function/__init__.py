import datetime
import logging
import requests
import json

from constants import BING_API_KEY

import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    tickers = ["MSFT", "AAPL", "AVGO", "IBM", "TXN", "DOCN"]

    for ticker in tickers:
        # search for news about a stock ticker on Bing News
        search_url = "https://api.bing.microsoft.com/v7.0/news/search"

        headers = {"Ocp-Apim-Subscription-Key" : BING_API_KEY}
        params  = {"q": ticker, "textDecorations": True, "textFormat": "HTML", "mkt": "en-US"}

        response = requests.get(search_url, headers=headers, params=params)
        response.raise_for_status()
        response_json = response.json()

        # convert results into json file
        file_name = f"{ticker}-{datetime.datetime.today()}.json"

        with open(file_name, "w") as f:
            json.dump(response_json, f)

        # connect and authenticate to the blob client
        account_url = "https://mlstorageleo.blob.core.windows.net"
        default_credential = DefaultAzureCredential()

        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient(account_url, credential=default_credential)

        blob_client = blob_service_client.get_blob_client(container="stock-news-json", blob=file_name)

        # upload created file 
        with open(file=f"./{file_name}", mode="rb") as data:
            blob_client.upload_blob(data)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
