from transformers import LlavaNextProcessor
from transformers import LlavaNextForConditionalGeneration
from PIL import Image
import json
import random
import numpy as np
import torch
import pandas as pd
from sklearn.model_selection import train_test_split
import os
import csv
from pathlib import Path
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, confusion_matrix
import gc
import re
from transformers import BitsAndBytesConfig



user_file = "./files/260515_users.csv"
content_file = "./files/260515_content.csv"
media_dir = "./media"
CSV_file = "./out/experiment_results.csv"
SEED_VALUE = 22
MAX_IMAGES = 3

random.seed(SEED_VALUE)
np.random.seed(SEED_VALUE)
torch.manual_seed(SEED_VALUE)
torch.cuda.manual_seed_all(SEED_VALUE)

FIELD_NAMES = [
    'experiment_type',
    'version',
    'experiment_name',
    'best_threshold',
    'val_accuracy',
    'val_f1',
    'val_precision',
    'val_recall',
    'val_roc_auc',
    'test_accuracy',
    'test_f1',
    'test_precision',
    'test_recall',
    'test_roc_auc',
    'test_confusion_matrix',
    'num_users_test',
    'num_users_val'
]

processor = LlavaNextProcessor.from_pretrained("llava-hf/llava-v1.6-mistral-7b-hf")



model = LlavaNextForConditionalGeneration.from_pretrained("llava-hf/llava-v1.6-mistral-7b-hf", torch_dtype=torch.float16, low_cpu_mem_usage=True) 
model.to("cuda:0")


print(f"Model loaded. GPU memory used: {torch.cuda.memory_allocated(0) / 1e9:.2f} GB")

def classify_mistral(text, images=None, strategy="zs-s", examples=[]):
    
    if strategy == "zs-e-s":
        prompt = f"""You are a clinical psychologist. Classify this comment as either ADHD or NON-ADHD.
Comment: "{text}"
Respond with ONLY 'ADHD' or 'NON-ADHD':"""

    elif strategy == "zs-s-p":
        prompt = f"""Classify this comment as either ADHD or NON-ADHD.
Comment: "{text}"
Respond with JSON: {{"label": "ADHD" or "NON-ADHD", "confidence":0.0-1.0}}"""

    elif strategy == "zs-e-p":
        prompt = f"""You are a clinical psychologist. Classify this comment as either ADHD or NON-ADHD.
Comment: "{text}"
Respond with JSON: {{"label": "ADHD" or "NON-ADHD", "confidence":0.0-1.0}}"""
        

    elif strategy == "fs-s":
        prompt = f"""Classify this comment as either ADHD or NON-ADHD.
Examples:
Comment: {examples[0]}
Response: ADHD
Comment: {examples[1]}
Response: ADHD
Comment: {examples[2]}
Response: NON-ADHD
Comment: {examples[3]}
Response: NON-ADHD

Now classify this comment:
Comment: "{text}"
Respond with ONLY 'ADHD' or 'NON-ADHD':"""


    else:
        prompt = f"""Classify this comment as either ADHD or NON-ADHD.
Comment: "{text}"
Respond with ONLY 'ADHD' or 'NON-ADHD':"""
    
    
    
    open_images = []
    
    if images:
        for path in images:
            try:
                image = Image.open(path).convert("RGB")
                #image.thumbnail((128, 128)) #resize
                open_images.append(image)
                
            except Exception as e:
                ...
                #print(f"Could not load image {path}: {e}")
    
    messages = [
        {
            "role": "user",
            "content": (
                [{"type": "image"} for _ in open_images]
                +
                [{
                    "type": "text",
                    "text": prompt
                }]
            )
        }
    ]
    
    prompt = processor.apply_chat_template(messages, add_generation_prompt=True)

    
    if open_images:
        inputs = processor(
            images=open_images,
            text=prompt,
            return_tensors="pt"
        ).to("cuda:0")
    else:
        inputs = processor(
            text=prompt,
            return_tensors="pt"
        ).to("cuda:0")

    
    with torch.inference_mode():
        outputs = model.generate(
            **inputs,
            max_new_tokens=16,
            do_sample=False,
            use_cache=True,
            eos_token_id=processor.tokenizer.eos_token_id,
            pad_token_id=processor.tokenizer.eos_token_id
        )
    
    
    prompt_len = inputs.input_ids.shape[1]
    generated_tokens = outputs[:, prompt_len:]

    result_text = processor.batch_decode(
        generated_tokens,
        skip_special_tokens=True
    )[0].strip().lower()
    
    result_text = result_text.replace("assistant", "").strip()

    
    if strategy.endswith("-s"):
        if result_text == "non-adhd":
            result = 0, 1.0
        elif result_text == "adhd":
            result = 1, 1.0
        else:
            result = -1, 0.0

    else:
        try:
            match = re.search(r'\{.*\}', result_text, re.DOTALL)

            data = json.loads(match.group())
                            
            raw_label = data.get('label', '').strip().lower()

            confidence = data.get('confidence', 0.5)
            
            if not (0 <= confidence <= 1):
                label = -1
                confidence = 0
            
            elif raw_label in ['adhd']:
                label = 1
            elif raw_label in ['non-adhd', 'non_adhd', 'non adhd']:
                label = 0
            else:
                label = -1
                confidence = 0

            
            result =  label, confidence
        except:
            result = -1, 0.0
    
    for img in open_images:
        img.close()

    del inputs
    del outputs
    del generated_tokens
    for img in open_images:
        img.close()

    del open_images

    return result
    
def classify_user_posts(df, strategy="zs-s", examples=[], use_images=True):
    """Devuelve proporcion de ADHD por usuario"""
    results = []
    idx=0
    for row in df.itertuples(index=False):
        idx+=1
        text = row.text
        has_media = row.has_media
        
        images = None
        content_path = f"{media_dir}/{row.id_y}"
        path_obj = Path(content_path)

        if  use_images and has_media and path_obj.exists():
            images = [str(image.resolve()) for image in list(path_obj.iterdir())[:MAX_IMAGES]]
        else:
            images = None
        
        label, confidence = classify_mistral(text, images=images, strategy=strategy, examples=examples)
                
        results.append({
            'user_id': row.id_x,
            'post_id': row.id_y,
            'label': label,
            'confidence': confidence,
            'true_label': row.has_ADHD
        })
        
        if idx % 100 == 0:
            gc.collect()
            torch.cuda.empty_cache()
    
    return pd.DataFrame(results)

def aggregate_by_user(predictions_df, threshold):
    user_results = []
    
    for user_id, group in predictions_df.groupby('user_id'):
        confidence_sum = group['confidence'].sum()

        if confidence_sum == 0:
            adhd_proportion = 0.5
        else:
            adhd_proportion = (
                (group['label'] * group['confidence']).sum()
                / confidence_sum
            )
        
        predicted_label = 1 if adhd_proportion >= threshold else 0
        
        true_label = group['true_label'].iloc[0]
        
        user_results.append({
            'user_id': user_id,
            'true_label': true_label,
            'predicted_label': predicted_label,
            'adhd_proportion': adhd_proportion
        })
    
    return pd.DataFrame(user_results)

def calculate_metrics(user_df):
    y_true = user_df['true_label']
    y_pred = user_df['predicted_label']
    
    metrics = {
        'accuracy': accuracy_score(y_true, y_pred),
        'f1': f1_score(y_true, y_pred, average='binary', zero_division=0),
        'precision': precision_score(y_true, y_pred, average='binary', zero_division=0),
        'recall': recall_score(y_true, y_pred, average='binary', zero_division=0),
    }
    
    try:
        metrics['roc_auc'] = roc_auc_score(y_true, user_df['adhd_proportion'])
    except:
        metrics['roc_auc'] = 0.5
    
    return metrics, confusion_matrix(y_true, y_pred)

def find_best_threshold(val_predictions_df, thresholds=np.arange(0.1, 0.91, 0.05)):
    best_threshold = 0.5
    best_f1 = 0
    
    for threshold in thresholds:
        user_results = aggregate_by_user(val_predictions_df, threshold)
        metrics, _ = calculate_metrics(user_results)
        
        if metrics['f1'] > best_f1:
            best_f1 = metrics['f1']
            best_threshold = threshold
    
    return best_threshold, best_f1    
    
def run_experiment(val,test,examples=None):
    all_results = []
    
    experiments = [
        #Few shot was not tested
        {'name': 'zs_text_only', 'strategy': 'zs-s', 'use_images': False},
        #{'name': 'Expert Zero-Shot (text only)', 'strategy': 'zs-e-s', 'use_images': False},
        {'name': 'zs_with_media', 'strategy': 'zs-s', 'use_images': True},
        {'name': 'zs_confidence_text_only', 'strategy': 'zs-s-p', 'use_images': False},
        #{'name': 'Expert Zero-Shot with confidence (text only)', 'strategy': 'zs-e-p', 'use_images': False},
        #{'name': 'Few-Shot (text only)', 'strategy': 'fs-s', 'use_images': False},
        #{'name': 'Expert Zero-Shot (with media)', 'strategy': 'zs-e-s', 'use_images': True},
        {'name': 'zs_confidence_with_media', 'strategy': 'zs-s-p', 'use_images': True},
        #{'name': 'Expert Zero-Shot with confidence (with media)', 'strategy': 'zs-e-p', 'use_images': True},
        #{'name': 'Few-Shot (with media)', 'strategy': 'fs-s', 'use_images': True}
    ]
    
    for exp_config in experiments:
        print(f"Running experiment: {exp_config['name']}")
        
        strategy = exp_config['strategy']
        
        #val
        val_pred = classify_user_posts(val, strategy, examples, use_images=exp_config['use_images'])
        best_threshold, val_f1 = find_best_threshold(val_pred)
        
        val_user_results = aggregate_by_user(val_pred, best_threshold)
        val_metrics, _ = calculate_metrics(val_user_results)
        
        #guardo resultados en csv
        CSV_val_file = f"./out/{exp_config['name']}_val.csv"
        val_predictions_to_save = val_user_results[['user_id', 'true_label', 'adhd_proportion', 'predicted_label']].copy()
        val_predictions_to_save.rename(columns={
            'user_id': 'user',
            'true_label': 'true_label',
            'adhd_proportion': 'predicted_probability',
            'predicted_label': 'predicted_label'
        }, inplace=True)
        val_predictions_to_save['dataset'] = 'validation'
        val_predictions_to_save.to_csv(CSV_val_file, index=False)
        
        #test
        test_pred = classify_user_posts(test, strategy, examples, use_images=exp_config['use_images'])
        
        test_user_results = aggregate_by_user(test_pred, best_threshold)
        test_metrics, test_cm = calculate_metrics(test_user_results)
        
        #guardo resultados en csv
        CSV_test_file = f"./out/{exp_config['name']}_test.csv"
        test_predictions_to_save = test_user_results[['user_id', 'true_label', 'adhd_proportion', 'predicted_label']].copy()
        test_predictions_to_save.rename(columns={
            'user_id': 'user',
            'true_label': 'true_label',
            'adhd_proportion': 'predicted_probability',
            'predicted_label': 'predicted_label'
        }, inplace=True)
        test_predictions_to_save['dataset'] = 'test'
        test_predictions_to_save.to_csv(CSV_test_file, index=False)
        
        result = {
            'experiment_type': "multimodal",
            'version': 1,
            'experiment_name': exp_config['name'],
            'best_threshold': best_threshold,
            'val_accuracy': val_metrics['accuracy'],
            'val_f1': val_metrics['f1'],
            'val_precision': val_metrics['precision'],
            'val_recall': val_metrics['recall'],
            'val_roc_auc': val_metrics['roc_auc'],
            'test_accuracy': test_metrics['accuracy'],
            'test_f1': test_metrics['f1'],
            'test_precision': test_metrics['precision'],
            'test_recall': test_metrics['recall'],
            'test_roc_auc': test_metrics['roc_auc'],
            'test_confusion_matrix': f"[[{test_cm[0,0]}, {test_cm[0,1]}], [{test_cm[1,0]}, {test_cm[1,1]}]]",
            'num_users_test': len(test_user_results),
            'num_users_val': len(val_user_results)
        }
        
        all_results.append(result)
        
        with open(CSV_file, 'a', newline='',encoding='utf-8') as file:
            writer = csv.DictWriter(file,fieldnames=FIELD_NAMES)
            writer.writerow(result)
    
    gc.collect()
    torch.cuda.empty_cache()
    
    return all_results

    

users_df = pd.read_csv(user_file)
content_df = pd.read_csv(content_file)
join_data = users_df.merge(content_df,left_on="id",right_on="user",how="left")

no_adhd_text = join_data.drop(join_data[join_data["has_ADHD_pattern"] == True].index)#[:200]

#elimino vacias
no_adhd_text = no_adhd_text.replace('', np.nan)
no_adhd_text = no_adhd_text[~no_adhd_text["text"].fillna("").str.isspace()]

no_adhd_text = no_adhd_text.dropna(subset=['text'])
no_adhd_text = no_adhd_text[no_adhd_text['text'].astype(str).str.strip() != '']
no_adhd_text = no_adhd_text[no_adhd_text['text'].astype(str).str.len() > 0]

no_adhd_text['text'] = no_adhd_text['text'].astype(str)

user_labels = no_adhd_text[['id_x', 'has_ADHD']].drop_duplicates()

#train test val 70 15 15
train_users, temp_users = train_test_split(
    user_labels,
    test_size=0.30,
    random_state=SEED_VALUE,
    stratify=user_labels["has_ADHD"]
)

val_users, test_users = train_test_split(
    temp_users,
    test_size=0.50,
    random_state=SEED_VALUE,
    stratify=temp_users["has_ADHD"]
)

train = no_adhd_text[no_adhd_text['id_x'].isin(train_users['id_x'])]

val = no_adhd_text[
    no_adhd_text['id_x'].isin(val_users['id_x'])
]

test = no_adhd_text[
    no_adhd_text['id_x'].isin(test_users['id_x'])
]

#escojo 2 ejemplos de cada para few-shot en train
adhd_examples = train[
    train["has_ADHD"] == True
].sample(2, random_state=SEED_VALUE)

non_adhd_examples = train[
    train["has_ADHD"] == False
].sample(2, random_state=SEED_VALUE)

examples = list(adhd_examples["text"]) + list(non_adhd_examples["text"])

file_exists = os.path.isfile(CSV_file)
with open(CSV_file, 'a', newline='',encoding='utf-8') as file:
    writer = csv.DictWriter(file,fieldnames=FIELD_NAMES)
    
    if not file_exists:
        writer.writeheader()

run_experiment(val,test,examples)


