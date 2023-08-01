
from pathlib import Path
import datetime 
import argparse
import json
import os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.communication.email import EmailClient

parser = argparse.ArgumentParser()
parser.add_argument("--notify_input", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--notify_output", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()

credential = DefaultAzureCredential()
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

conenction_string = secret_client.get_secret("mail-connection-string")
email_client = EmailClient.from_connection_string(conenction_string.value)

with open(os.path.join(args.notify_input, "merged_stock_news.json"), "r") as f:
      data_string = json.load(f)

data = json.loads(data_string)

nl = "\n"
msft_summaries = " ".join([i+nl for i in data["MSFT"]["summaries"] if "All photographs subject to copyright." not in i])
msft_sentiments = (data["MSFT"]["sentiments"].count("positive"), data["MSFT"]["sentiments"].count("neutral"), data["MSFT"]["sentiments"].count("negative"))
msft_texts = " ".join([i+nl for i in data["MSFT"]["url"]])

text = f"""
This is your daily stock news summary. 

News about Microsoft: 
{msft_summaries}
\n Sentiments: positive -> {msft_sentiments[0]} neutral -> {msft_sentiments[1]}, negative -> {msft_sentiments[2]}

Full texts: 
Microsoft 
{msft_texts}

"""
# News about DigitalOcean:
# {" ".join([i+nl for i in data["DOCN"]["summaries"] if "All photographs subject to copyright." not in i])}
# \n Sentiments: positive -> {data["DOCN"]["sentiments"].count("positive")}, neutral -> {data["DOCN"]["sentiments"].count("neutral")}, negative -> {data["DOCN"]["sentiments"].count("negative")}

# News about Apple:
# {" ".join([i+nl for i in data["AAPL"]["summaries"] if "All photographs subject to copyright." not in i])}
# \n Sentiments: positive -> {data["AAPL"]["sentiments"].count("positive")}, neutral -> {data["AAPL"]["sentiments"].count("neutral")}, negative -> {data["AAPL"]["sentiments"].count("negative")}

# News about Boradcom: 
# {" ".join([i+nl for i in data["AVGO"]["summaries"] if "All photographs subject to copyright." not in i])}
# \n Sentiments: positive -> {data["AVGO"]["sentiments"].count("positive")}, neutral -> {data["AVGO"]["sentiments"].count("neutral")}, negative -> {data["AVGO"]["sentiments"].count("negative")}

# News about IBM:
# {" ".join([i+nl for i in data["IBM"]["summaries"] if "All photographs subject to copyright." not in i])}
# \n Sentiments: positive -> {data["IBM"]["sentiments"].count("positive")}, neutral -> {data["IBM"]["sentiments"].count("neutral")}, negative -> {data["IBM"]["sentiments"].count("negative")}

# News about Texas Instruments:
# {" ".join([i+nl for i in data["TXN"]["summaries"] if "All photographs subject to copyright." not in i])}
# \n Sentiments: positive -> {data["TXN"]["sentiments"].count("positive")}, neutral -> {data["TXN"]["sentiments"].count("neutral")}, negative -> {data["TXN"]["sentiments"].count("negative")}


# DigialOcean
# {" ".join([i+nl for i in data["DOCN"]["url"]])}

# Apple
# {" ".join([i+nl for i in data["AAPL"]["url"]])}

# Broadcom
# {" ".join([i+nl for i in data["AVGO"]["url"]])}

# IBM 
# {" ".join([i+nl for i in data["IBM"]["url"]])}

# Texas Instruments 
# {" ".join([i+nl for i in data["TXN"]["url"]])}

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
print(poller.result())

# overwrite old files with new files containing the sentiment
with open((Path(args.notify_output) / "merged_stock_news.json"), "w") as f:
      json.dump(data, f)
