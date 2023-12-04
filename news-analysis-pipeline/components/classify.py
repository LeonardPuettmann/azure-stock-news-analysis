
from pathlib import Path
import argparse
import json
import os

import transformers
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Define constants for model names
DESTILBERT_MODEL_NAME = "KernAI/stock-news-destilbert"
FINBERT_MODEL_NAME = "ProsusAI/finbert"

# Define a dictionary to map model names to their tokenizers
MODEL_NAME_TO_TOKENIZER = {
    DESTILBERT_MODEL_NAME: AutoTokenizer,
    FINBERT_MODEL_NAME: AutoTokenizer,
}

# Define a dictionary to map model names to their models
MODEL_NAME_TO_MODEL = {
    DESTILBERT_MODEL_NAME: AutoModelForSequenceClassification,
    FINBERT_MODEL_NAME: AutoModelForSequenceClassification,
}

def download_model(model_name: str):
    model = MODEL_NAME_TO_MODEL[model_name].from_pretrained(model_name)
    tokenizer = MODEL_NAME_TO_TOKENIZER[model_name].from_pretrained(model_name)
    return model, tokenizer

def use_model(
    model, 
    tokenizer,
    text: str
    ) -> str:
    tokenized_text = tokenizer(
        text,
        truncation=True,
        is_split_into_words=False,
        return_tensors="pt"
    )

    outputs = model(**tokenized_text)
    outputs_logits = outputs.logits.argmax(1)

    if isinstance(model, transformers.models.distilbert.modeling_distilbert.DistilBertForSequenceClassification):
        mapping = {0: 'neutral', 1: 'negative', 2: 'positive'} # distilbert mapping
    else:
        mapping = {0: 'positive', 1: 'negative', 2: 'neutral'} # finbert mapping

    return mapping[int(outputs_logits[0])]

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--classify_input", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--classify_output", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()

# Download models
destilbert_model, destilbert_tokenizer = download_model(DESTILBERT_MODEL_NAME)
finbert_model, finbert_tokenizer = download_model(FINBERT_MODEL_NAME)

# Read input data
input_file_path = os.path.join(args.classify_input, "merged_stock_news.json")
with open(input_file_path, "r") as f:
    data = json.load(f)

# Iterate through tickers
for ticker, ticker_data in data.items():
    texts = ticker_data["texts"]

    # Use the models and append sentiments
    sentiments_distilbert = [use_model(destilbert_model, destilbert_tokenizer, text) for text in texts]
    sentiments_finbert = [use_model(finbert_model, finbert_tokenizer, text) for text in texts]

    # Update the data with sentiments
    ticker_data["sentiments"] = sentiments_distilbert
    ticker_data["sentiments_finbert"] = sentiments_finbert

# Write the updated data back to the output file
output_file_path = Path(args.classify_output) / "merged_stock_news.json"
with open(output_file_path, "w") as f:
    json.dump(data, f)
