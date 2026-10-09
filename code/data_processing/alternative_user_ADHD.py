'''
Adds alternative ways to detect if a user mentions having ADHD.
'''

import pandas as pd
import torch
from transformers import pipeline, BitsAndBytesConfig

in_content_file = "../../in/content.csv"
in_users_file = "../../in/users.csv"
out_users_file = "../../out/users_with_altmethod.csv"
out_content_file = "../../out/content_with_altmethod.csv"

LLM_model = "Qwen/Qwen3-0.6B"
sarcasm_thr = 0.7 #If user has >0.7 prob of being sarcastic, label = 0

content_df = pd.read_csv(in_content_file)
users_df = pd.read_csv(in_users_file)

quant_conf = BitsAndBytesConfig(load_in_8bit=True)


pipe = pipeline(task="text-generation",
                      model=LLM_model,
                      device=0 if torch.cuda.is_available() else -1,
                      torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                      model_kwargs={"quantization_config": quant_conf} if torch.cuda.is_available() else {}
)

def model_label(row):
    '''
    Uses a LLM to determine if user discloses ADHD status.
    '''
    
    prompt = f"""You are classifying whether the SPEAKER of a text asserts that they personally have ADHD.

Answer YES only if the speaker claims ADHD as their own condition, e.g.:
- "I have ADHD"
- "I was diagnosed with ADHD"
- "My ADHD makes it hard to focus"

Answer NO in all other cases, including:
- ADHD mentioned about someone else: "my friend has ADHD"
- Questions: "do I have ADHD?"
- Negation: "I don't have ADHD"
- Uncertainty: "I think I might have ADHD" / "I probably have ADHD"
- Hypotheticals/jokes: "I'm so ADHD today lol"
- General info: "ADHD affects about 5% of children"
- Testing/medication without diagnosis claim: "I'm getting tested for ADHD"

Text: "{row['text']}"

Answer with exactly one word: YES or NO.
Answer:"""

    try:
        response = pipe(
            prompt,
            max_new_tokens=3,
            do_sample=False,
            return_full_text=False,
            pad_token_id=pipe.tokenizer.eos_token_id
        )
        
        answer = response[0]['generated_text'].strip().upper()
        
        if "YES" in answer:
            row["has_ADHD_pattern_model"] = True
        else:
            row["has_ADHD_pattern_model"] = False
        
    except Exception as e:
        print(f"Error processing: {e}")
        row["has_ADHD_pattern_model"] = False

    return row

def sarcasm_label(row):
    '''
    Takes the sarcasm prob. and pattern recognition into account to determine if user says they have ADHD.
    '''
    row["has_ADHD_pattern_sarcasm"] = row["has_ADHD_pattern"] and (row["sarcasm_prob"] < sarcasm_thr )
    row["has_ADHD_pattern_model_sarcasm"] = row["has_ADHD_pattern_model"] and (row["sarcasm_prob"] < sarcasm_thr )
    return row
    

def update_users(row):
    '''
    Adds new "has_ADHD" attributes according to model and sarcasm.
    '''
    row["has_ADHD_model"] = content_df.loc[(content_df["user"]==row["id"]),"has_ADHD_pattern_model"].any()
    row["has_ADHD_sarcasm"] = content_df.loc[(content_df["user"]==row["id"]),"has_ADHD_pattern_sarcasm"].any()
    row["has_ADHD_model_sarcasm"] = content_df.loc[(content_df["user"]==row["id"]),"has_ADHD_pattern_model_sarcasm"].any()
    return row

content_df["text"] = content_df["text"].astype(str).fillna("")
content_df = content_df.apply(model_label,axis=1)
content_df = content_df.apply(sarcasm_label,axis=1)

users_df = users_df.apply(update_users,axis=1)

content_df.to_csv(out_content_file,index=False)
users_df.to_csv(out_users_file,index=False)