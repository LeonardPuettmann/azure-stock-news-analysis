
from pathlib import Path
import datetime 
import argparse
import json
import os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.communication.email import EmailClient

# Define constants for the email subject and sender address
EMAIL_SUBJECT = f"Stock news analysis for {datetime.datetime.today().isoformat()[:10]}"
SENDER_ADDRESS = "DoNotReply@632a8f5c-5cc8-4c44-8e7e-f509c76d0d24.azurecomm.net"
RECIPIENT_ADDRESS = "leopuettmann@outlook.de"

parser = argparse.ArgumentParser()
parser.add_argument("--notify_input", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--notify_output", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()

with open(os.path.join(args.notify_input, "merged_stock_news.json"), "r") as f:
    data = json.load(f)

# print(data)
# print(type(data))

def format_data(ticker):
    global data
    summaries = " ".join(data[ticker]["summaries"])
    sentiments = (
        data[ticker]["sentiments"].count("positive"),
        data[ticker]["sentiments"].count("neutral"),
        data[ticker]["sentiments"].count("negative"),
    )
    texts = " ".join(data[ticker]["url"])
    return summaries, sentiments, texts

msft_summaries, msft_sentiments, msft_texts = format_data("MSFT")
aapl_summaries, aapl_sentiments, aapl_texts = format_data("AAPL")
txn_summaries, txn_sentiments, txn_texts = format_data("TXN")

email_content = f"""
This is your daily stock news summary. 

===
News about Microsoft: 
{msft_summaries}
\n Sentiments: positive -> {msft_sentiments[0]} | neutral -> {msft_sentiments[1]} | negative -> {msft_sentiments[2]}
=== \n\n

===
News about Apple: 
{aapl_summaries}
\n Sentiments: positive -> {aapl_sentiments[0]} | neutral -> {aapl_sentiments[1]} | negative -> {aapl_sentiments[2]}
=== \n\n

===
News about Texas Instruments: 
{txn_summaries}
\n Sentiments: positive -> {txn_sentiments[0]} | neutral -> {txn_sentiments[1]} | negative -> {txn_sentiments[2]}
===
"""

def send_email(email_client, subject, content, recipient, sender):
    message = {
        "content": {
            "subject": subject,
            "plainText": content
        },
        "recipients": {
            "to": [
                {
                    "address": recipient,
                    "displayName": "Leo"
                }
            ]
        },
        "senderAddress": sender
    }
    poller = email_client.begin_send(message)

# Initialize Azure services and clients
credential = DefaultAzureCredential()
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)
connection_string = secret_client.get_secret("mail-connection-string")
email_client = EmailClient.from_connection_string(connection_string.value)

# Send email
send_email(email_client, EMAIL_SUBJECT, email_content, RECIPIENT_ADDRESS, SENDER_ADDRESS)

# Pass merged stock news JSON file to the output of the pipeline
with open((Path(args.notify_output) / "merged_stock_news.json"), "w") as f:
    json.dump(data, f)
