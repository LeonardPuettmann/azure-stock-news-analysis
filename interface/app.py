import streamlit as st 

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import plotly.graph_objects as go

import yfinance as yf

from os.path import exists
import datetime
import json

credential = DefaultAzureCredential()
# Check if given credential can get token successfully.
credential.get_token("https://management.azure.com/.default")
secret_client = SecretClient(vault_url="https://mlgroup.vault.azure.net/", credential=credential)

# log in to the Blob Service Client
blob_storage_key = secret_client.get_secret("blob-storage-key")
blob_service_client = BlobServiceClient("https://mlstorageleo.blob.core.windows.net/", account_key=blob_storage_key.value)

# connect to the container 
container_client = blob_service_client.get_container_client(container="processed-stock-news-json") 

# list and download all currently available blobs
blob_list = container_client.list_blobs()
available_dates = [datetime.datetime.strptime(str(file_name.name)[21:31],"%Y-%m-%d") for file_name in blob_list] # processed-stock-news-2023-07-22.json
selected_date = st.sidebar.date_input("Start date", value=max(available_dates), min_value=min(available_dates), max_value=max(available_dates), format="YYYY-MM-DD")

tickers = ["MSFT", "AAPL", "DOCN", "AVGO", "TXN", "IBM"]
ticker_symbol = st.sidebar.selectbox('Stock ticker', tickers) 

if ticker_symbol and selected_date:
# for blob in blob_list:
    if not exists(f"interface/blobs/processed-stock-news-{selected_date}.json"):
        blob_client = blob_service_client.get_blob_client(container="processed-stock-news-json", blob=f"processed-stock-news-{selected_date}.json")
        with open(f"interface/blobs/processed-stock-news-{selected_date}.json", mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

    with open(f"interface/blobs/processed-stock-news-{selected_date}.json", mode="r+") as f:
        data = json.load(f)

    summaries = data[ticker_symbol]["summaries"]
    summaries = [i for i in summaries if "All photographs subject to copyright." not in i]
    st.write(summaries)

    col1, col2, col3 = st.columns(3)

    col1.metric("positive sentiments", data[ticker_symbol]["sentiments"].count("positive"))
    col2.metric("neutral sentiments", data[ticker_symbol]["sentiments"].count("neutral"))
    col3.metric("negative sentiments", data[ticker_symbol]["sentiments"].count("negative"))


    data = yf.download(ticker_symbol, start=selected_date, end=selected_date + datetime.timedelta(days=1), interval="1m")
    fig = go.Figure(data=go.Ohlc(x=data.index,
                    open=data["Open"],
                    high=data["High"],
                    low=data["Low"],
                    close=data["Close"],
    ))

    st.plotly_chart(fig, use_container_width=True)


            

