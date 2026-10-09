'''
Adds sarcasm values to each content item.
'''

import pandas as pd
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
import string
import torch

in_file = "../../in/content.csv"
out_file = "../../out/content_with_sarcasm.csv"

content_df = pd.read_csv(in_file)


def preprocess_data(text: str) -> str:
   return text.lower().translate(str.maketrans("", "", string.punctuation)).strip()

MODEL_PATH = "helinivan/english-sarcasm-detector"
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)


def label_sarcasm(row):
    tokenized_text = tokenizer([preprocess_data(row["text"])], padding=True, truncation=True, max_length=512, return_tensors="pt")
    output = model(**tokenized_text)
    probs = output.logits
    probs = torch.softmax(probs,dim=-1).tolist()[0]
    sarcasm_value = probs[1]
    row["sarcasm_prob"] = sarcasm_value
        
    return row

content_df["text"] = content_df["text"].astype(str).fillna("")
content_df = content_df.apply(label_sarcasm,axis=1)

content_df.to_csv(out_file,index=False)