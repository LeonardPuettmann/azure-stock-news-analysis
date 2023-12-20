
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from pathlib import Path
import datetime 
import argparse
import json
import os
from openai import OpenAI

credential = DefaultAzureCredential()
# check if given credential can get token successfully.
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

parser = argparse.ArgumentParser()
parser.add_argument("--summarize_input", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--summarize_output", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()


# retriev the list of blobs from the current day - input is a .txt file
with open(os.path.join(args.summarize_input, "merged_stock_news.json"), "r") as f:
      data = json.load(f)

# authenticate to openai
api_key = api_key=secret_client.get_secret("openai-key")
openai_client = OpenAI(api_key=api_key.value)

# get a list of all tickers, summaries all texts for each ticker
tickers = list(data.keys())
for ticker in tickers:
    texts = data[ticker]["texts"]

    summaries = []
    for text in texts: 
        response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
                {"role": "system", "content": f"""
                    As an assistant, your task is to summarize stock and finance news. 
                    Your summary should be a single sentence that rephrases the key information without using phrases like "The article is about" or "The article discusses". 
                    When in doubt, leave information out. The summary should be short.
                    Be sure to include specific numbers such as stock price changes or concrete earning figures. Aim for brevity and precision in your summary.
                    =========
                    Article: {text}
                    =========
                    Summary: 
                    """}
            ],
            max_tokens=60, 
            temperature=0.0
        )

        summaries.append(response.choices[0].message.content)

    # add the sentiments to the data
    data[ticker]["summaries"] = summaries

# connect and authenticate to the blob client
account_url = "https://mlstorageleo.blob.core.windows.net"
file_name = f"processed-stock-news-{datetime.datetime.today().isoformat()[:10]}.json"

# create the BlobServiceClient object
blob_data = json.dumps(data)
blob_storage_key = secret_client.get_secret("blob-storage-key")
blob_service_client = BlobServiceClient(account_url, credential=blob_storage_key.value)
blob_client = blob_service_client.get_blob_client(container="processed-stock-news-json", blob=file_name)
blob_client.upload_blob(blob_data)

# overwrite old files with new files containing the sentiment
with open((Path(args.summarize_output) / "merged_stock_news.json"), "w") as f:
      json.dump(data, f)
