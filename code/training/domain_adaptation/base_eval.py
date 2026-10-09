'''
Perplexity evaluation of base model
'''

import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModelForMaskedLM, DataCollatorForLanguageModeling, Trainer, TrainingArguments, set_seed, TrainerCallback
from sklearn.model_selection import train_test_split
import math
import random
import torch
from datasets import DatasetDict, Dataset


user_file = "../../in/users.csv"
content_file = "../../in/content.csv"

MIN_CONTENT = 15
MIN_WORDS = 30
SEED_VALUE = 22

random.seed(SEED_VALUE)
np.random.seed(SEED_VALUE)
torch.manual_seed(SEED_VALUE)
torch.cuda.manual_seed_all(SEED_VALUE)
set_seed(SEED_VALUE)

model_name = "FacebookAI/roberta-base"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForMaskedLM.from_pretrained(model_name)


users_df = pd.read_csv(user_file)
content_df = pd.read_csv(content_file,dtype={"image_description": str})
join_data = users_df.merge(content_df,left_on="id",right_on="user",how="left")


##---PREPROCESADO--##
#descartamos las que tienen pocas palabras/contenido
join_data = join_data[join_data["n_content"] >= MIN_CONTENT]    
join_data = join_data[join_data["word_count"] >= MIN_WORDS]

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

#sets para MLM
train_mlm, val_mlm = train_test_split(
    train,
    test_size=0.15,
    random_state=SEED_VALUE,
    stratify=train["has_ADHD"]
)

dataset = DatasetDict({
        "train": Dataset.from_pandas(train_mlm.reset_index(drop=True)),
        "validation": Dataset.from_pandas(val_mlm.reset_index(drop=True)),
    })

def preprocess_data(examples):
        encoding = tokenizer(
                examples["text"],
                #padding="max_length", 
                truncation=True, 
                max_length=256
            )
                
        return encoding


class NaNCheckCallback(TrainerCallback):
    def on_step_end(self, args, state, control, model=None, **kwargs):
        if model is None:
            return

        for name, param in model.named_parameters():
            if not torch.isfinite(param).all():
                print(
                    f"NON-FINITE PARAMETER AT STEP {state.global_step}: {name}"
                )
                raise RuntimeError(
                    f"NaN/Inf detected in parameter {name} "
                    f"at step {state.global_step}"
                )
                
class GradientCheckCallback(TrainerCallback):
    def on_pre_optimizer_step(self, args, state, control, model=None, **kwargs):
        if model is None:
            return

        for name, param in model.named_parameters():
            if param.grad is not None and not torch.isfinite(param.grad).all():
                print(
                    f"NON-FINITE GRADIENT at step {state.global_step}: {name}"
                )
                raise RuntimeError(
                    f"Non-finite gradient in {name}"
                )

encoded_dataset = dataset.map(preprocess_data, batched=True, remove_columns=dataset['train'].column_names)
encoded_dataset.set_format("torch")


data_collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=True, mlm_probability=0.15,seed=SEED_VALUE)

#entrenamiento
training_args = TrainingArguments(
    output_dir="../../out/roberta-base-eval",
    eval_strategy = "epoch",
    save_strategy = "epoch",
    #overwrite_output_dir=True,
    num_train_epochs=5,
    learning_rate=5e-5,
    per_device_train_batch_size=4,
    per_device_eval_batch_size=4,
    seed=SEED_VALUE,
    #save_steps=500,
    gradient_checkpointing=False,
    gradient_accumulation_steps=4,
    dataloader_num_workers=0,
    fp16=False,
    warmup_steps=0.1,
    bf16=False,
    weight_decay=0.01,
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
    greater_is_better=False,
    data_seed=SEED_VALUE,
    save_total_limit=1,
    max_grad_norm=1.0,
    logging_strategy="steps",
    logging_steps=1,
    do_train=False,
    do_eval=True,
    report_to="none",
)


trainer = Trainer(
    model=model,
    args=training_args,
    eval_dataset=encoded_dataset["validation"],
    data_collator=data_collator,
    callbacks=[NaNCheckCallback(),GradientCheckCallback()],
)



eval_results = trainer.evaluate()
perpexity = math.exp(eval_results["eval_loss"])
print(f"Perplexity: {perpexity}")
