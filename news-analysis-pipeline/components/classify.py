
import argparse
import json
import os

from transformers import AutoTokenizer, AutoModelForSequenceClassification

parser = argparse.ArgumentParser()
parser.add_argument("--blob_storage_read", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--blobs_to_use_output", type=str, help="Mounted Azure ML blob storage")
parser.add_argument("--blob_storage_write", type=str, help="Mounted Azure ML blob storage")
args = parser.parse_args()

# download distilbert model from HuggingFace
tokenizer = AutoTokenizer.from_pretrained("KernAI/stock-news-destilbert")
model = AutoModelForSequenceClassification.from_pretrained("KernAI/stock-news-destilbert")

def main():
      # retriev the list of blobs from the current day - input is a .txt file
      with open(args.blob_storage, "r") as f:
            blobs_to_use = f.read()


      dir_list = args.folder_path
      for file_name in [file for file in os.listdir(dir_list) if file in blobs_to_use]:
            with open(dir_list + file_name) as json_file:
                  data = json.load(json_file)
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
            with open(dir_list+file_name, "w") as f:
                  json.dump(data, f)

            # Note: no dedicated output needed here: we'll take the output from the first component again for the next step

if __name__ == "__main__":
      main()
