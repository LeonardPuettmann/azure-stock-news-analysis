import logging
import datetime
import json

import azure.functions as func

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.communication.email import EmailClient
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

conenction_string = secret_client.get_secret("mail-connection-string")
email_client = EmailClient.from_connection_string(conenction_string.value)

blob_storage_key = secret_client.get_secret("blob-storage-key")
blob_service_client = BlobServiceClient("https://mlstorageleo.blob.core.windows.net/", account_key=blob_storage_key.value)
container_client = blob_service_client.get_container_client(container="processed-stock-news-json") 


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    today = datetime.datetime.today().isoformat()[:10]
    
    blob_client = blob_service_client.get_blob_client(container="processed-stock-news-json", blob=f"processed-stock-news-{today}.json")
    with open(f"tmp/processed-stock-news-{today}.json", mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())

    with open(f"tmp/processed-stock-news-{today}.json", mode="r+") as f:
        data = json.load(f)

    nl = "\n"
    text = f"""
    This is your daily stock news summary. 

    News about Microsoft: 
    {" ".join([i+nl for i in data["MSFT"]["summaries"] if "All photographs subject to copyright." not in i])}
    \n Sentiments: positive -> {data["MSFT"]["sentiments"].count("positive")}, neutral -> {data["MSFT"]["sentiments"].count("neutral")}, negative -> {data["MSFT"]["sentiments"].count("negative")}

    News about DigitalOcean:
    {" ".join([i+nl for i in data["DOCN"]["summaries"] if "All photographs subject to copyright." not in i])}
    \n Sentiments: positive -> {data["DOCN"]["sentiments"].count("positive")}, neutral -> {data["DOCN"]["sentiments"].count("neutral")}, negative -> {data["DOCN"]["sentiments"].count("negative")}

    News about Apple:
    {" ".join([i+nl for i in data["AAPL"]["summaries"] if "All photographs subject to copyright." not in i])}
    \n Sentiments: positive -> {data["AAPL"]["sentiments"].count("positive")}, neutral -> {data["AAPL"]["sentiments"].count("neutral")}, negative -> {data["AAPL"]["sentiments"].count("negative")}

    News about Boradcom: 
    {" ".join([i+nl for i in data["AVGO"]["summaries"] if "All photographs subject to copyright." not in i])}
    \n Sentiments: positive -> {data["AVGO"]["sentiments"].count("positive")}, neutral -> {data["AVGO"]["sentiments"].count("neutral")}, negative -> {data["AVGO"]["sentiments"].count("negative")}

    News about IBM:
    {" ".join([i+nl for i in data["IBM"]["summaries"] if "All photographs subject to copyright." not in i])}
    \n Sentiments: positive -> {data["IBM"]["sentiments"].count("positive")}, neutral -> {data["IBM"]["sentiments"].count("neutral")}, negative -> {data["IBM"]["sentiments"].count("negative")}

    News about Texas Instruments:
    {" ".join([i+nl for i in data["TXN"]["summaries"] if "All photographs subject to copyright." not in i])}
    \n Sentiments: positive -> {data["TXN"]["sentiments"].count("positive")}, neutral -> {data["TXN"]["sentiments"].count("neutral")}, negative -> {data["TXN"]["sentiments"].count("negative")}

    Full texts: 
    Microsoft 
    {" ".join([i+nl for i in data["MSFT"]["url"]])}

    DigialOcean
    {" ".join([i+nl for i in data["DOCN"]["url"]])}

    Apple
    {" ".join([i+nl for i in data["AAPL"]["url"]])}

    Broadcom
    {" ".join([i+nl for i in data["AVGO"]["url"]])}

    IBM 
    {" ".join([i+nl for i in data["IBM"]["url"]])}

    Texas Instruments 
    {" ".join([i+nl for i in data["TXN"]["url"]])}
    """
    
    message = {
    "content": {
        "subject": f"Stock news analysis for {datetime.datetime.today().isoformat()[:10]}",
        "plainText": text
    },
    "recipients": {
        "to": [
            {
                "address": "leopuettmann@outlook.de",
                "displayName": "Leo"
            }
        ]
    },
    "senderAddress": "DoNotReply@632a8f5c-5cc8-4c44-8e7e-f509c76d0d24.azurecomm.net"
    }

    poller = email_client.begin_send(message)
    print("Result: " + poller.result())

    
    
