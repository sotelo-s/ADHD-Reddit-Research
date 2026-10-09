'''
    Calculate metrics.
'''

from sklearn.metrics import roc_curve, auc, roc_auc_score, precision_recall_curve, average_precision_score,f1_score,brier_score_loss,log_loss,accuracy_score,precision_score,recall_score,confusion_matrix
import pandas as pd
import numpy as np

users_pred_file = "../../out/experiments/users_predictions.csv"
content_pred_file = "../../out/experiments/content_predictions.csv"
out_file = "../../out/experiments/metrics.csv"

users_df = pd.read_csv(users_pred_file)
content_df = pd.read_csv(content_pred_file)


result = []
for exp_name in users_df["experiment_name"].unique():
    df_val = users_df[(users_df["experiment_name"]==exp_name)&(users_df["dataset"]=="validation")]
    df_test = users_df[(users_df["experiment_name"]==exp_name)&(users_df["dataset"]=="test")]
    

    #calculo threshold que maximiza f1 en val
    if len(df_val["predicted_probability"].unique()) > 2:
        precision,recall,thresholds = precision_recall_curve(df_val["true_label"],df_val["predicted_probability"])

        f1_scores = np.divide(
            2*precision*recall,
            precision+recall,
            out=np.zeros_like(precision),
            where=(precision+recall)!=0
        )

        best_idx = np.argmax(f1_scores[:-1])
        best_threshold = thresholds[best_idx]
    else:
        best_threshold = 1.0

    #calculo la etiqueta
    df_val["pred_label"] = (df_val["predicted_probability"] >= best_threshold).astype(int)
    df_test["pred_label"] = (df_test["predicted_probability"] >= best_threshold).astype(int)
    
    result.append({
        "experiment_type" : df_val.iloc[0]["experiment_type"],
        "experiment_name" : exp_name,
        "level" : "user", 
        "dataset" : "validation",
        "brier_score" : brier_score_loss(df_val["true_label"],df_val["predicted_probability"]),
        "log_loss" : log_loss(df_val["true_label"],df_val["predicted_probability"]),
        "roc_auc_score" : roc_auc_score(df_val["true_label"],df_val["predicted_probability"]),
        "average_precision_score" : average_precision_score(df_val["true_label"],df_val["predicted_probability"]),
        "threshold" : best_threshold,
        "f1_score":f1_score(df_val["true_label"],df_val["pred_label"]),
        "accuracy_score" : accuracy_score(df_val["true_label"],df_val["pred_label"]),
        "precision_score": precision_score(df_val["true_label"],df_val["pred_label"]),
        "recall_score": recall_score(df_val["true_label"],df_val["pred_label"]),
        "confusion_matrix": confusion_matrix(df_val["true_label"],df_val["pred_label"]).tolist(),
        #"val_f1" : f1_score(df_val["true_label"],df_val["predicted_probability"])
    })

    result.append({
            "experiment_type" : df_test.iloc[0]["experiment_type"],
            "experiment_name" : exp_name,
            "level" : "user", 
            "dataset" : "test",
            "brier_score" : brier_score_loss(df_test["true_label"],df_test["predicted_probability"]),
            "log_loss" : log_loss(df_test["true_label"],df_test["predicted_probability"]),
            "roc_auc_score" : roc_auc_score(df_test["true_label"],df_test["predicted_probability"]),
            "average_precision_score" : average_precision_score(df_test["true_label"],df_test["predicted_probability"]),
            "threshold" : best_threshold,
            "f1_score":f1_score(df_test["true_label"],df_test["pred_label"]),
            "accuracy_score" : accuracy_score(df_test["true_label"],df_test["pred_label"]),
            "precision_score": precision_score(df_test["true_label"],df_test["pred_label"]),
            "recall_score": recall_score(df_test["true_label"],df_test["pred_label"]),
            "confusion_matrix": confusion_matrix(df_test["true_label"],df_test["pred_label"]).tolist(),
        })




result_df = pd.DataFrame(result)

result_df.to_csv(out_file,index=False)
