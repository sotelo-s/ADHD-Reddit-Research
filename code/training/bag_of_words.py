import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import random
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import f1_score, accuracy_score, precision_score, roc_auc_score, recall_score, confusion_matrix
import csv, os



user_file = "./files/260515_users.csv"
content_file = "./files/260515_content.csv"
CSV_file = "./out/experiment_results.csv"
SEED_VALUE = 22


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

random.seed(SEED_VALUE)
np.random.seed(SEED_VALUE)





def run_experiment(data_train,data_val,data_test, name, subreddits_to_remove=None, min_words=0, min_content=0, version=2):
    train = data_train.copy()
    val = data_val.copy()
    test = data_test.copy()
    
    if subreddits_to_remove:
        train = train[~train["first_found_in"].isin(subreddits_to_remove)]
    
        
    if min_words > 0:
        train = train[train["word_count"] >= min_words]

    if min_content > 0:
        train = train[train["n_content"] >= min_content]
        
    


    vectorizer = CountVectorizer(
        stop_words='english',
        max_features=5000,
        min_df=2,
        max_df=0.95
    )

    X_train = vectorizer.fit_transform(train["text"])
    X_val = vectorizer.transform(val["text"])
    X_test = vectorizer.transform(test["text"])

    y_train = train["has_ADHD"].astype(int)
    y_val = val["has_ADHD"].astype(int)
    y_test = test["has_ADHD"].astype(int)

    classifier = MultinomialNB()
    classifier.fit(X_train,y_train)
    
    val_preds = classifier.predict(X_val)
    val_probs = classifier.predict_proba(X_val)[:,1]
    
    test_preds = classifier.predict(X_test)
    test_probs = classifier.predict_proba(X_test)[:,1]
    
    
    val_text_metrics = {
        'accuracy': accuracy_score(y_val, val_preds),
        'f1': f1_score(y_val, val_preds, average='binary'),
        'precision': precision_score(y_val, val_preds, average='binary'),
        'recall': recall_score(y_val, val_preds, average='binary'),
        'roc_auc': roc_auc_score(y_val, val_probs)
    }
    

    
    #metricas a nivel de usuario
    
    val_df = val.copy()
    val_df['pred_label'] = val_preds
    val_df['pred_proba_true'] = val_probs
    
    #agregar por usuario
    def aggregate_users(df):
        user_stats = df.groupby('user').agg(
            mean_proba=('pred_proba_true', 'mean'),
            real_label=('has_ADHD', 'first')
        ).reset_index()

        return user_stats
    
    user_val = aggregate_users(val_df)
    
    #buscar el mejor umbral
    thresholds = np.arange(0.1, 0.91, 0.05)
    best_f1 = 0
    best_threshold = 0.5
    best_val_user_metrics = None
    
    for threshold in thresholds:
        user_val['pred_user_adhd'] = (user_val['mean_proba'] >= threshold).astype(int)
        
        if len(np.unique(user_val['real_label'])) > 1:  #ambas clases
            f1_temp = f1_score(user_val['real_label'], user_val['pred_user_adhd'])
            if f1_temp > best_f1:
                best_f1 = f1_temp
                best_threshold = threshold
                best_val_user_metrics = {
                    'accuracy': accuracy_score(user_val['real_label'], user_val['pred_user_adhd']),
                    'f1': f1_temp,
                    'precision': precision_score(user_val['real_label'], user_val['pred_user_adhd']),
                    'recall': recall_score(user_val['real_label'], user_val['pred_user_adhd']),
                    'roc_auc': roc_auc_score(user_val['real_label'], user_val['mean_proba'])
                }
    

    #aplica el mejor umbral a la parte de test
    test_df = test.copy()
    test_df['pred_label'] = test_preds
    test_df['pred_proba_true'] = test_probs
    
    user_test = aggregate_users(test_df)
    user_test['pred_user_adhd'] = (user_test['mean_proba'] >= best_threshold).astype(int)
    user_val['pred_user_adhd'] = (user_val['mean_proba'] >= best_threshold).astype(int)
    
    
    #guardo predicciones val
    CSV_val_file = f"./out/{name}_val.csv"
    val_predictions_to_save = user_val[['user', 'real_label', 'mean_proba', 'pred_user_adhd']].copy()
    val_predictions_to_save.rename(columns={
        'real_label': 'true_label',
        'mean_proba': 'predicted_probability',
        'pred_user_adhd': 'predicted_label'
    }, inplace=True)
    val_predictions_to_save['dataset'] = 'validation'
    val_predictions_to_save.to_csv(CSV_val_file, index=False)
    
    #guardo predicciones test
    CSV_test_file = f"./out/{name}_test.csv"
    test_predictions_to_save = user_test[['user', 'real_label', 'mean_proba', 'pred_user_adhd']].copy()
    test_predictions_to_save.rename(columns={
        'real_label': 'true_label',
        'mean_proba': 'predicted_probability',
        'pred_user_adhd': 'predicted_label'
    }, inplace=True)
    test_predictions_to_save['dataset'] = 'test'
    test_predictions_to_save.to_csv(CSV_test_file, index=False)
    
    
    
    #metricas de usuario a nivel de usuario
    test_user_metrics = {
        'accuracy': accuracy_score(user_test['real_label'], user_test['pred_user_adhd']),
        'f1': f1_score(user_test['real_label'], user_test['pred_user_adhd']),
        'precision': precision_score(user_test['real_label'], user_test['pred_user_adhd']),
        'recall': recall_score(user_test['real_label'], user_test['pred_user_adhd']),
        'roc_auc': roc_auc_score(user_test['real_label'], user_test['mean_proba']) if len(np.unique(user_test['real_label'])) > 1 else 0.5
    }
    
    
    
   
    cm = confusion_matrix(user_test['real_label'], user_test['pred_user_adhd'])
    
    result = {
        'experiment_type': 'bag_of_words',
        'version': version,
        'experiment_name': name,
        'best_threshold': best_threshold,
        'val_accuracy': best_val_user_metrics['accuracy'] if best_val_user_metrics else None,
        'val_f1': best_val_user_metrics['f1'] if best_val_user_metrics else None,
        'val_precision': best_val_user_metrics['precision'] if best_val_user_metrics else None,
        'val_recall': best_val_user_metrics['recall'] if best_val_user_metrics else None,
        'val_roc_auc': best_val_user_metrics['roc_auc'] if best_val_user_metrics else None,
        'test_accuracy': test_user_metrics['accuracy'],
        'test_f1': test_user_metrics['f1'],
        'test_precision': test_user_metrics['precision'],
        'test_recall': test_user_metrics['recall'],
        'test_roc_auc': test_user_metrics['roc_auc'],
        'test_confusion_matrix': f"[[{cm[0,0]}, {cm[0,1]}], [{cm[1,0]}, {cm[1,1]}]]",
        'num_users_test': len(user_test),
        'num_users_val': len(user_val)
    }
    
    all_results.append(result)
    

users_df = pd.read_csv(user_file)
content_df = pd.read_csv(content_file)
join_data = users_df.merge(content_df,left_on="id",right_on="user",how="left")



#eliminar texto que tenga patron TDAH
no_adhd_text = join_data.drop(join_data[join_data["has_ADHD_pattern"] == True].index)#[:1000]

#elimino vacias
no_adhd_text = no_adhd_text.replace('', np.nan)
no_adhd_text = no_adhd_text[~no_adhd_text["text"].str.isspace()]

no_adhd_text = no_adhd_text.dropna(subset=['text'])
no_adhd_text = no_adhd_text[no_adhd_text['text'].astype(str).str.strip() != '']
no_adhd_text = no_adhd_text[no_adhd_text['text'].astype(str).str.len() > 0]

no_adhd_text['text'] = no_adhd_text['text'].astype(str)
    
    
all_results = []

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

data_train = no_adhd_text[no_adhd_text['user'].isin(train_users['user'])]
data_val = no_adhd_text[no_adhd_text['user'].isin(val_users['user'])]
data_test = no_adhd_text[no_adhd_text['user'].isin(test_users['user'])]
 
 
run_experiment(data_train,data_val,data_test, "BOW_all_sources",None,min_words=0, min_content=0)
run_experiment(data_train,data_val,data_test, "BOW_removed_off_topic",["r/self","r/CasualConversation"],min_words=0, min_content=0)
run_experiment(data_train,data_val,data_test, "BOW_all_sources_threshold",None,min_words=19, min_content=24)
run_experiment(data_train,data_val,data_test, "BOW_removed_off_topic_threshold",["r/self","r/CasualConversation"],min_words=29, min_content=25)

file_exists = os.path.isfile(CSV_file)
with open(CSV_file, 'a', newline='',encoding='utf-8') as file:
    writer = csv.DictWriter(file,fieldnames=FIELD_NAMES)
    
    if not file_exists:
        writer.writeheader()
        
    for exp in all_results:
        writer.writerow(exp)
