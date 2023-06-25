
import argparse
import json
import os

from transformers import AutoTokenizer, AutoModelForSequenceClassification

parser = argparse.ArgumentParser()
parser.add_argument("--input_data", type=str, help="path or URL to input data")
parser.add_argument("--output_data", type=str, help="path or URL to output data")
args = parser.parse_args()

# download distilbert model from HuggingFace
tokenizer = AutoTokenizer.from_pretrained("KernAI/stock-news-destilbert")
model = AutoModelForSequenceClassification.from_pretrained("KernAI/stock-news-destilbert")

def main():
      #dir_list = os.listdir(args.input_data)
      dir_list = args.input_data
      for file_name in [file for file in os.listdir(dir_list) if file.endswith('.json')]:
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

if __name__ == "__main__":
      main()
