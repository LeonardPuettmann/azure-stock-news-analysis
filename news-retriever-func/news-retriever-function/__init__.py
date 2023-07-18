import datetime
import logging
import requests
import json

from newspaper import Article

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import azure.functions as func
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    tickers = ["MSFT", "AAPL", "DOCN", "AVGO", "TXN", "IBM"]

    for ticker in tickers:
        # search for news about a stock ticker on Bing News
        search_url = "https://api.bing.microsoft.com/v7.0/news/search"

        bing_key = secret_client.get_secret("bing-key")
        headers = {"Ocp-Apim-Subscription-Key" : bing_key.value}
        params  = {"q": ticker, "textDecorations": True, "textFormat": "HTML", "mkt": "en-US"}

        response = requests.get(search_url, headers=headers, params=params)
        response.raise_for_status()
        response_json = response.json()

        
        article_info = {}
        get_these = ["name", "description", "datePublished", "url"]

        for i in get_these:
                content = [response_json["value"][j][i] for j in range(len(response_json["value"]))]
                article_info[i] = content

        scraped_articles = []
        for url in article_info["url"]:
                try: 
                    article = Article(url)

                    article.download()
                    article.parse()

                    text = article.text
                    scraped_articles.append(" ".join(text.split()[:100]))
                except:
                    scraped_articles.append(" ")

        article_info["texts"] = scraped_articles
        article_info["ticker"] = ticker * len(article_info["texts"])

        # store dict as a json string
        file_name = f"{ticker}-{datetime.datetime.today().timestamp()}.json"
        data = json.dumps(article_info)

        # connect and authenticate to the blob client
        account_url = "https://mlstorageleo.blob.core.windows.net"

        # Create the BlobServiceClient object
        blob_storage_key = secret_client.get_secret("blob-storage-key")
        blob_service_client = BlobServiceClient(account_url, credential=blob_storage_key.value)
        blob_client = blob_service_client.get_blob_client(container="stock-news-json", blob=file_name)
        blob_client.upload_blob(data)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
