import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split
from transformers import AutoTokenizer, DataCollatorWithPadding
from datasets import DatasetDict, Dataset
from transformers import AutoModelForSequenceClassification
from transformers import TrainingArguments, Trainer
from transformers import EvalPrediction
import torch
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, confusion_matrix
import random
import csv




user_file = "./files/260515_users.csv"
content_file = "./files/260515_content.csv"
CSV_file = "./out/experiment_results.csv"
SEED_VALUE = 22

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



def run_experiment(data_train,data_val,data_test, name, subreddits_to_remove=None, min_words=0, min_content=0, version=1):
    
    train = data_train.copy()
    
    if subreddits_to_remove:
        #eliminar subreddits origen
        train = train[~train["first_found_in"].isin(subreddits_to_remove)]


    #aplicar umbral
    if min_content > 0:
        train = train[train["n_content"] >= min_content]
        
    if min_words > 0:
        train = train[train["word_count"] >= min_words]

        
    dataset = DatasetDict({
        "train": Dataset.from_pandas(train.reset_index(drop=True)),
        "validation": Dataset.from_pandas(data_val.reset_index(drop=True)),
        "test": Dataset.from_pandas(data_test.reset_index(drop=True))
    })



    tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
    data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

    labels = ["True","False"]
    id2label = {1: "True", 0: "False"}
    label2id = {"True":1,"False":0}

    def preprocess_data(examples):
        # take a batch of texts
        texts = examples["text"]
        # encode them
        encoding = tokenizer(
                texts,
                #padding="max_length", 
                truncation=True, 
                #max_length=128
            )
        
        encoding["labels"] = [int(label) for label in examples["has_ADHD"]]
        encoding["user"] = examples["user"]
        encoding["has_ADHD"] = examples["has_ADHD"]
        encoding["text"] = examples["text"]
            
        return encoding




    encoded_dataset = dataset.map(preprocess_data, batched=True, remove_columns=dataset['train'].column_names)
    encoded_dataset.set_format("torch")



    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-uncased",
        num_labels=2,
        id2label=id2label,
        label2id=label2id
    )



    batch_size = 32
    metric_name = "f1"

    args = TrainingArguments(
        f"bert-finetuned-sem_eval-english",
        eval_strategy = "epoch",
        save_strategy = "epoch",
        learning_rate=2e-5,
        fp16=True,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        num_train_epochs=2,
        weight_decay=0.01,
        load_best_model_at_end=True,
        metric_for_best_model=metric_name,
        seed=SEED_VALUE,
        data_seed=SEED_VALUE,
        #push_to_hub=True,
    )


    def compute_metrics(p: EvalPrediction):
        preds = p.predictions[0] if isinstance(p.predictions,
                tuple) else p.predictions
        
        y_pred = np.argmax(preds,axis=1)
        y_true = p.label_ids
        
        accuracy = accuracy_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred, average='binary')
        precision = precision_score(y_true, y_pred, average='binary')
        recall = recall_score(y_true, y_pred, average='binary')
        
        probs = torch.softmax(torch.tensor(preds), dim=1).numpy()
        proba_positive = probs[:, 1]  #probabilidad true
        roc_auc = roc_auc_score(y_true, proba_positive)
        
        return {
            'accuracy': accuracy,
            'f1': f1,
            'precision': precision,
            'recall': recall,
            'roc_auc': roc_auc
        }


    outputs = model(input_ids=encoded_dataset['train']['input_ids'][0].unsqueeze(0), labels=encoded_dataset['train'][0]['labels'].unsqueeze(0))

    trainer = Trainer(
        model,
        args,
        train_dataset=encoded_dataset["train"],
        eval_dataset=encoded_dataset["validation"],
        #tokenizer=tokenizer,
        data_collator=data_collator,
        compute_metrics=compute_metrics
    )

    trainer.train()



    #busco el mejor umbral
    val_predictions = trainer.predict(encoded_dataset["validation"])
    y_pred_probs_val = torch.nn.functional.softmax(torch.tensor(val_predictions.predictions), dim=-1)
    y_pred_binary_val = np.argmax(val_predictions.predictions, axis=1)

    val_df = encoded_dataset["validation"].to_pandas()
    val_df['pred_label'] = y_pred_binary_val
    val_df['pred_proba_true'] = y_pred_probs_val[:, 1].numpy()

    #agrupo por usuarios
    user_val = val_df.groupby('user').agg(
        mean_proba=('pred_proba_true', 'mean'),
        real_label=('has_ADHD', 'first')
    ).reset_index()




    thresholds = np.arange(0.1, 0.91, 0.05)
    best_f1 = 0
    best_threshold = 0.5
    best_val_user_metrics = None


    for threshold in thresholds:
        user_val['pred_temp'] = (
            user_val['mean_proba'] >= threshold
        ).astype(int)
        f1_temp = f1_score(user_val['real_label'], user_val['pred_temp'])
        if f1_temp > best_f1:
            best_f1 = f1_temp
            best_threshold = threshold
            best_val_user_metrics = {
                    'accuracy': accuracy_score(user_val['real_label'], user_val['pred_temp']),
                    'f1': f1_temp,
                    'precision': precision_score(user_val['real_label'], user_val['pred_temp']),
                    'recall': recall_score(user_val['real_label'], user_val['pred_temp']),
                    'roc_auc': roc_auc_score(user_val['real_label'],user_val['mean_proba']),
                }

    user_val['pred_user_adhd'] = (user_val['mean_proba'] >= best_threshold).astype(int)

    y_true_users_val = user_val['real_label']
    y_pred_users_val = user_val['pred_user_adhd']


    test_predictions = trainer.predict(encoded_dataset["test"])
    y_pred_probs_test = torch.nn.functional.softmax(torch.tensor(test_predictions.predictions), dim=-1)
    y_pred_binary_test = np.argmax(test_predictions.predictions, axis=1)

    test_df = encoded_dataset["test"].to_pandas()
    test_df['pred_label'] = y_pred_binary_test
    test_df['pred_proba_true'] = y_pred_probs_test[:, 1].numpy()

    user_test = test_df.groupby('user').agg(
        mean_proba=('pred_proba_true', 'mean'),
        real_label=('has_ADHD', 'first')
    ).reset_index()

    user_test['pred_user_adhd'] = (
        user_test['mean_proba'] >= best_threshold
    ).astype(int)



    y_true_users = user_test['real_label']
    y_pred_users = user_test['pred_user_adhd']


    test_user_metrics = {
        'accuracy': accuracy_score(y_true_users, y_pred_users),
        'f1': f1_score(y_true_users, y_pred_users),
        'precision': precision_score(y_true_users, y_pred_users),
        'recall': recall_score(y_true_users, y_pred_users),
        'roc_auc': roc_auc_score(y_true_users,user_test['mean_proba']),
    }
    cm = confusion_matrix(y_true_users, y_pred_users)

    result = {
        'experiment_type': 'bert',
        'version': version,
        'experiment_name': name,
        'best_threshold': best_threshold,
        'val_accuracy': best_val_user_metrics['accuracy'],
        'val_f1': best_val_user_metrics['f1'],
        'val_precision': best_val_user_metrics['precision'],
        'val_recall': best_val_user_metrics['recall'],
        'val_roc_auc': best_val_user_metrics['roc_auc'],
        'test_accuracy': test_user_metrics['accuracy'],
        'test_f1': test_user_metrics['f1'],
        'test_precision': test_user_metrics['precision'],
        'test_recall': test_user_metrics['recall'],
        'test_roc_auc': test_user_metrics['roc_auc'],
        'test_confusion_matrix': f"[[{cm[0,0]}, {cm[0,1]}], [{cm[1,0]}, {cm[1,1]}]]",
        'num_users_test': len(user_test),
        'num_users_val': len(user_val),
    }

    #guardar predicciones val
    val_predictions_to_save = user_val[['user', 'real_label', 'mean_proba', 'pred_user_adhd']].copy()
    val_predictions_to_save.rename(columns={
        'real_label': 'true_label',
        'mean_proba': 'predicted_probability',
        'pred_user_adhd': 'predicted_label'
    }, inplace=True)
    val_predictions_to_save['dataset'] = 'validation'
    val_predictions_to_save.to_csv(f"./out/{name}_val.csv", index=False)

    #guardar predicciones test
    test_predictions_to_save = user_test[['user', 'real_label', 'mean_proba', 'pred_user_adhd']].copy()
    test_predictions_to_save.rename(columns={
        'real_label': 'true_label',
        'mean_proba': 'predicted_probability',
        'pred_user_adhd': 'predicted_label'
    }, inplace=True)
    test_predictions_to_save['dataset'] = 'test'
    test_predictions_to_save.to_csv(f"./out/{name}_test.csv", index=False)


    #guardar metricas
    file_exists = os.path.isfile(CSV_file)
    with open(CSV_file, 'a', newline='',encoding='utf-8') as file:
        writer = csv.DictWriter(file,fieldnames=FIELD_NAMES)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(result)
        
        
        
users_df = pd.read_csv(user_file)
content_df = pd.read_csv(content_file)
join_data = users_df.merge(content_df,left_on="id",right_on="user",how="left")

#eliminar texto que tenga patron TDAH
no_adhd_text = join_data.drop(join_data[join_data["has_ADHD_pattern"] == True].index)#[:200]

#elimino vacias
no_adhd_text = no_adhd_text.replace('', np.nan)
no_adhd_text = no_adhd_text[~no_adhd_text["text"].str.isspace()]

no_adhd_text = no_adhd_text.dropna(subset=['text'])
no_adhd_text = no_adhd_text[no_adhd_text['text'].astype(str).str.strip() != '']
no_adhd_text = no_adhd_text[no_adhd_text['text'].astype(str).str.len() > 0]

no_adhd_text['text'] = no_adhd_text['text'].astype(str)

user_labels = no_adhd_text[['user', 'has_ADHD']].drop_duplicates()

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

train = no_adhd_text[no_adhd_text['user'].isin(train_users['user'])]
val = no_adhd_text[no_adhd_text['user'].isin(val_users['user'])]
test = no_adhd_text[no_adhd_text['user'].isin(test_users['user'])]

run_experiment(train,val,test,"BERT_all_sources",None,0,0,1)
run_experiment(train,val,test,"BERT_removed_off_topic",["r/self","r/CasualConversation"],0,0,1)
run_experiment(train,val,test,"BERT_all_sources_threshold",None,29,17,2)
run_experiment(train,val,test,"BERT_removed_off_topic_threshold",["r/self","r/CasualConversation"],29,16,2)
