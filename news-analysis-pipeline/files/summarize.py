import argparse
import json
import os

from transformers import PegasusTokenizer, PegasusForConditionalGeneration, TFPegasusForConditionalGeneration

# parser = argparse.ArgumentParser()
# parser.add_argument("--input_data", type=str, help="path or URL to input data")
# parser.add_argument("--output_data", type=str, help="path or URL to output data")
# args = parser.parse_args()

# load the model and the tokenizer
tokenizer = PegasusTokenizer.from_pretrained("human-centered-summarization/financial-summarization-pegasus")
model = PegasusForConditionalGeneration.from_pretrained("human-centered-summarization/financial-summarization-pegasus") 

def main():
      #dir_list = os.listdir(args.input_data)
      dir_list = "./data/"
      for file_name in [file for file in os.listdir(dir_list) if file.endswith('.json')]:
            with open(dir_list + file_name) as json_file:
                  data = json.load(json_file)
            texts = data["texts"]

            summaries = []
            for text in texts: 
                # Tokenize our text
                # If you want to run the code in Tensorflow, please remember to return the particular tensors as simply as using return_tensors = 'tf'
                input_ids = tokenizer(text_to_summarize, return_tensors="pt").input_ids

                # Generate the output (Here, we use beam search but you can also use any other strategy you like)
                output = model.generate(
                    input_ids, 
                    max_length=32, 
                    num_beams=5, 
                    early_stopping=True
                )

                # Finally, we can print the generated summary
                print(tokenizer.decode(output[0], skip_special_tokens=True))

            # add the sentiments to the data
            # data["sentiments"] = sentiments

            # # overwrite old files with new files containing the sentiment
            # with open(dir_list+file_name, "w") as f:
            #       json.dump(data, f)

if __name__ == "__main__":
      main()
