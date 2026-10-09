'''
Mask-phrase prediction comparison between non-adapted and adapted to the domain models. 
'''

import torch
from transformers import pipeline, AutoTokenizer, AutoModelForMaskedLM
import pandas as pd

base_model = "FacebookAI/roberta-base"
adapted_model = "../../out/roberta-domain-adapted"

out_dir = "../../out/fillmask_comparison.csv"

phrases = [
    ("I <mask> to finish projects",["struggle","fail","forget"]),
    ("I can't get things in order for a task needing <mask>",["organization","planning","structure","focus","order"]),
    ("I forget my <mask>",["appointments","obligations","commitments","plans","tasks","responsibilities"]),
    ("I <mask> starting hard tasks",["avoid","delay","postpone","dread","struggle"]),
    ("I <mask> with my hands and feet when I'm sitting'",["fidget","squirm","wiggle","tap"]),
    ("I feel overly <mask> to do things",["active","driven","restless","compelled","motivated"]),
    ("I make <mask> on boring work",["mistakes","errors","slips","blunders"]),
    ("I can't <mask> when doing repetitive work",["concentrate","focus","persist","engage"]),
    ("I can't <mask> on what people say",["concentrate","focus","attend"]),
    ("I often <mask> my things",["misplace","lose","forget"]),
    ("I feel <mask> by noise",["distracted"]),
    ("I feel <mask>",["restless","fidgety","agitated","impatient"]),
    ("I can't <mask>",["unwind","concentrate","relax","focus","settle"]),
    ("I talk too <mask>",["much","often","excessively"]),
    ("I <mask> others' sentences",["finish","complete","interrupt","end","cut"]),
    ("I can't wait my <mask>",["turn"]),
    ("I <mask> other's when they are busy",["interrupt"])    
]

def reciprocal_rank(predictions,expected):
    for rank, pred in enumerate(predictions, start=1):
        pred_token = pred["token_str"].strip().lower()
        hit = any(exp in pred_token or pred_token in exp for exp in expected)
        if hit:
            return 1.0/rank
    return 0.0

def evaluate_model(model_name,phrases,top_k=10):
    tokenizer = AutoTokenizer.from_pretrained(model_name,return_tensors="pt")
    model = AutoModelForMaskedLM.from_pretrained(model_name)
    device = 0 if torch.cuda.is_available() else -1
    fillmask = pipeline("fill-mask",model=model,tokenizer=tokenizer,top_k=top_k,device=device)

    mrr_scores = []
    results = []

    for prompt, answers in phrases:
        predictions = fillmask(prompt)
        rr = reciprocal_rank(predictions,answers)

        mrr_scores.append(rr)

        results.append({
            "model" : model_name,
            "prompt" : prompt,
            "expected" : answers,
            "rr" : rr,
            "top_predictions" : [p['token_str'].strip() for p in predictions]
        })

    mrr = sum(mrr_scores)/len(mrr_scores)
    return pd.DataFrame(results)

results_base = evaluate_model(base_model,phrases)
results_adapted = evaluate_model(adapted_model,phrases)

total = pd.concat([results_base,results_adapted],axis=0)

total.to_csv(out_dir,index=False)
