'''
Adds emotion values to each content item.
'''

import pandas as pd
import torch
from transformers import pipeline

in_file = "../../in/content.csv"
out_file = "../../out/content_with_emotion_and_sentiment.csv"

content_df = pd.read_csv(in_file)

emotion_classifier = pipeline("text-classification", 
                      model="j-hartmann/emotion-english-distilroberta-base", 
                      top_k=None,
                      truncation=True,
                      max_length=512,
                      device=0 if torch.cuda.is_available() else -1)

sentiment_classifier = pipeline("text-classification", 
                      model="j-hartmann/sentiment-roberta-large-english-3-classes", 
                      top_k=None,
                      truncation=True,
                      max_length=512,
                      device=0 if torch.cuda.is_available() else -1)


def label_emotion_and_sentiment(row):
    emotion_result = emotion_classifier(row["text"])[0]
    sentiment_result = sentiment_classifier(row["text"])[0]
    
    emotion_data = {item["label"]: item["score"] for item in emotion_result}
    sentiment_data = {item["label"]: item["score"] for item in sentiment_result}
    
    for label in emotion_data.keys():
        row[f"emotion_{label}"] = emotion_data[label]
    
    for label in sentiment_data.keys():
        row[f"sentiment_{label}"] = sentiment_data[label]
        
    return row

content_df["text"] = content_df["text"].astype(str).fillna("")
content_df = content_df.apply(label_emotion_and_sentiment,axis=1)

content_df.to_csv(out_file,index=False)