'''
    Procesa los ficheros de usuarios y contenido para eliminar usuarios sin contenido o viceversa
    y añade datos agregados.
    
    Uso: python data_cleaning.py <user_data_out.csv> <content_data_out.csv> <user_data_in.csv> <content_data_in.csv> <media_folder>
'''

import pandas as pd
import sys
from langdetect import detect
import os
import shutil

def clean_data(users_df,content_df,media_folder,ud_filename_out,c_filename_out):
    
    
    content_df= remove_not_english(content_df)
    content_df= remove_images(content_df)
    content_df= remove_links(content_df)
    content_df= remove_mentions(content_df)
    content_df= remove_spoiler_tags(content_df)
    content_df= remove_age_and_gender(content_df)
    users_df, content_df= remove_empty(users_df,content_df)
    
    users_df, content_df = fix(users_df,content_df,media_folder)
    
    users_df,content_df = add_extra_data(users_df,content_df)
    
    users_df.to_csv(ud_filename_out,index=False)
    content_df.to_csv(c_filename_out,index=False)
    
    
    
def fix(users_df,content_df,media_folder):
    #Elimino contenido del que no tengo usuarios
    missing_users = set(content_df['user']) - set(users_df['id'])
    
    if missing_users:
        content_df.drop(content_df[content_df['user'].isin(missing_users)].index,inplace=True)
     
    content_ids = set(content_df['id'].astype(str))
    deleted_folders = []
    
    if os.path.exists(media_folder):
        for folder_name in os.listdir(media_folder):
            folder_path = os.path.join(media_folder, folder_name)
            
            #elimino carpetas de usuarios/contenido eliminados
            if os.path.isdir(folder_path):
                if folder_name not in content_ids:
                    try:
                        shutil.rmtree(folder_path)
                        deleted_folders.append(folder_name)
                    except Exception as e:
                        print(f"Error: {e}")
            
            #elimino carpetas vacias en media
                elif not os.listdir(folder_path):
                    try:
                        os.rmdir(folder_path)
                        deleted_folders.append(folder_name)
                    except Exception as e:
                         print(f"Error: {e}")
                         
    
                        
    #cambio columna "has_media" a False si elimino la carpeta
    if 'has_media' not in content_df.columns:
        content_df['has_media'] = False
    
    mask = content_df['id'].astype(str).isin(deleted_folders)
    content_df.loc[mask, 'has_media'] = False
    
    return users_df,content_df
        

    
def remove_not_english(content_df):
    def is_english(text):
        try:
            if pd.isna(text) or not isinstance(text, str):
                return False
            return detect(text) == 'en'
        except:
            return False
        
    
    is_english_mask = content_df['text'].apply(is_english)
    remove_mask = ~is_english_mask & ~content_df['has_media']
    content_df.drop(content_df[remove_mask].index, inplace=True)
    
    return content_df


def remove_images(content_df):
    content_df['text'] = content_df['text'].str.replace({r'<image>':''}, regex=True)
    return content_df


def remove_links(content_df):
    content_df['text'] = content_df['text'].str.replace({r'\[.*\]\(.*\)':''}, regex=True)
    return content_df


def remove_mentions(content_df):
    content_df['text'] = content_df['text'].str.replace({r'[ru]\/[^\s\/]+':''}, regex=True, case=False)
    return content_df
    
    
def remove_spoiler_tags(content_df):
    content_df['text'] = content_df['text'].str.replace({r'(>!)|(!<)':''}, regex=True)
    return content_df


def remove_age_and_gender(content_df):
    content_df['text'] = content_df['text'].str.replace({r'\(\s*(?:\d+\s*(?:[FM]|NB|non-?binary|male|female|other|\?)?|(?:[FM]|NB|non-?binary|male|female|other|\?)\s*\d+)\s*\)':''}, regex=True, case=False)
    return content_df
   
   
def remove_empty(users_df,content_df):
    content_df['text'] = content_df['text'].str.replace(r'\s+', ' ', regex=True)
    content_df['text'] = content_df['text'].str.strip()
    
    #elimino vacios
    content_df.drop(content_df[(content_df.text == '')&(content_df.get("has_media",False) == False)].index,inplace=True)
    content_df.dropna(inplace=True)
    
    #elimino usuarios que no tienen contenido
    users_with_content = set(content_df['user'].unique())
    users_df.drop(users_df[~users_df['id'].isin(users_with_content)].index, inplace=True)
    
    return users_df,content_df
    
    
    
def add_extra_data(users_df,content_df):
    #añado num de caracteres por contenido
    content_df["char_count"] = [len(str(t)) for t in content_df["text"]] 
    
    #añado num de palabras por contenido
    content_df["word_count"] = [len(str(t).split()) for t in content_df["text"]] 
    

    #añado num total de posts y comentarios por usuario
    content_sum = content_df.groupby(['user','type']).count().reset_index()[['user','type','id']]
    
    content_sum = content_sum.pivot_table(index='user',columns='type', values='id', fill_value=0)
    
    if 'post' not in content_sum.columns:
        content_sum['post'] = 0
    if 'comment' not in content_sum.columns:
        content_sum['comment'] = 0
        
        
    content_sum.rename(columns={'post': 'n_posts', 'comment': 'n_comments'},inplace=True)

    content_sum['n_posts'] = content_sum['n_posts'].astype(int)
    content_sum['n_comments'] = content_sum['n_comments'].astype(int)

    content_sum['n_content'] = content_sum['n_posts'] + content_sum['n_comments']
    
    users_df = users_df.merge(content_sum, left_on='id', right_index=True)
    
    #añado medias, medianas y desviacion tipica de caracteres y palabras por usuario
    len_data = content_df.groupby(["user","type"])[["char_count","word_count"]].agg(["mean","median","std"])
    len_data.columns = [f'{col.replace("_count", "")}_{agg}' for col, agg in len_data.columns]

    len_data = len_data.unstack(level='type')
    len_data.columns = [f'{col[0]}_{col[1]}' for col in len_data.columns]

    users_df = users_df.merge(len_data, left_on="id", right_on="user")
    
    return users_df, content_df
    

##-------------------CODIGO PRINCIPAL-------------------##

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Use: python data_cleaning.py <user_data_out.csv> <content_data_out.csv> <user_data_in.csv> <content_data_in.csv> <media_folder>")
        sys.exit(1)
        
    ud_filename_out = sys.argv[1]
    c_filename_out = sys.argv[2]
    ud_filename_in = sys.argv[3]
    c_filename_in = sys.argv[4]
    media_folder = sys.argv[5]
    
    users_df = pd.read_csv(ud_filename_in)
    content_df = pd.read_csv(c_filename_in)
    
    
    clean_data(users_df,content_df,media_folder,ud_filename_out,c_filename_out)
    