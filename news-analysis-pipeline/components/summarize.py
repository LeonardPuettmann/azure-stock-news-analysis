
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from pathlib import Path
import datetime 
import argparse
import json
import os

from transformers import PegasusTokenizer, PegasusForConditionalGeneration, TFPegasusForConditionalGeneration

credential = DefaultAzureCredential()
# Check if given credential can get token successfully.
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

parser = argparse.ArgumentParser()
parser.add_argument("--summarize_input", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--summarize_output", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()

# load the model and the tokenizer
tokenizer = PegasusTokenizer.from_pretrained("human-centered-summarization/financial-summarization-pegasus")
model = PegasusForConditionalGeneration.from_pretrained("human-centered-summarization/financial-summarization-pegasus") 

# retriev the list of blobs from the current day - input is a .txt file
with open(os.path.join(args.summarize_input, "merged_stock_news.json"), "r") as f:
      data = json.load(f)

tickers = list(data.keys())
for ticker in tickers:
      texts = data[ticker]["texts"]

      summaries = []
      for text in texts: 
            # Tokenize our text
            # If you want to run the code in Tensorflow, please remember to return the particular tensors as simply as using return_tensors = 'tf'
            input_ids = tokenizer(text, return_tensors="pt").input_ids

            # Generate the output (Here, we use beam search but you can also use any other strategy you like)
            output = model.generate(
                  input_ids, 
                  max_length=32, 
                  num_beams=5, 
                  early_stopping=True
            )

            # Finally, we can print the generated summary
            summaries.append(tokenizer.decode(output[0], skip_special_tokens=True))

      # add the sentiments to the data
      data[ticker]["summaries"] = summaries
      
data = json.dumps(data)

# connect and authenticate to the blob client
account_url = "https://mlstorageleo.blob.core.windows.net"
file_name = f"processed-stock-news-{datetime.datetime.today().isoformat()[:10]}.json"

# Create the BlobServiceClient object
blob_storage_key = secret_client.get_secret("blob-storage-key")
blob_service_client = BlobServiceClient(account_url, credential=blob_storage_key.value)
blob_client = blob_service_client.get_blob_client(container="processed-stock-news-json", blob=file_name)
blob_client.upload_blob(data)

# overwrite old files with new files containing the sentiment
with open((Path(args.summarize_output) / "merged_stock_news.json"), "w") as f:
      json.dump(data, f)
