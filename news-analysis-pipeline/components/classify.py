
from pathlib import Path
import argparse
import json
import os

from transformers import AutoTokenizer, AutoModelForSequenceClassification

parser = argparse.ArgumentParser()
parser.add_argument("--classify_input", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--classify_output", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()

# download distilbert model from HuggingFace
tokenizer = AutoTokenizer.from_pretrained("KernAI/stock-news-destilbert")
model = AutoModelForSequenceClassification.from_pretrained("KernAI/stock-news-destilbert")

# retriev the list of blobs from the current day - input is a .txt file
with open(args.classify_input, "r") as f:
      data = f.read()
texts = data["texts"]

sentiments = []
for text in texts: 
      tokenized_text = tokenizer(
            text,
            truncation=True,
            is_split_into_words=False,
            return_tensors="pt"
      )

      outputs = model(tokenized_text["input_ids"])
      outputs_logits = outputs.logits.argmax(1)

      mapping = {0: 'neutral', 1: 'negative', 2: 'positive'}
      predicted_label = mapping[int(outputs_logits[0])]
      sentiments.append(predicted_label)

# add the sentiments to the data
data["sentiments"] = sentiments

# overwrite old files with new files containing the sentiment
with open("merged_stock_news.json", "w") as f:
      json.dump(data, f)

(Path(args.prep_output) / "merged_stock_news.json")
