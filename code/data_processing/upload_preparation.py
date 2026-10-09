'''
    Removes broken and NSFW images, removes empty content with no images and censores faces.
    
    Use: python upload_preparation.py <user_data_out.csv> <content_data_out.csv> <user_data.csv> <content_data.csv> <media_folder> <NSFW_media_list.csv>
'''

import pandas as pd
import sys
import os
from media_anonymizer import clean_media
from data_cleaning import fix
from PIL import Image
import csv

def delete_broken_and_nsfw(media_folder,nsfw_file):
    nsfw_dict = {}
    
    try:
        nsfw_df = pd.read_csv(nsfw_file)
        
        for content_id, group in nsfw_df.groupby('content_id'):
            nsfw_dict[str(content_id)] = set(group['image'].values)
    except:
        ...
    
    folders = os.listdir(media_folder)
    
    for c_id in folders:
        folder_path = f"{media_folder}/{c_id}"
        nsfw_images = nsfw_dict.get(c_id, set())
            
        for image in os.listdir(folder_path):
            image_path = f"{folder_path}/{image}"
            
            #nsfw?
            if image in nsfw_images:
                os.remove(image_path)
                continue
            
            #imagen rota?
            try:
                with Image.open(image_path) as img:
                    img.verify()

                with Image.open(image_path) as img:
                    img.load()
            except FileNotFoundError:
                pass
            except pd.errors.EmptyDataError:
                pass
            except Exception:
                os.remove(image_path)
        
     

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Use: python upload_preparation.py <user_data_out.csv> <content_data_out.csv> <user_data.csv> <content_data.csv> <media_folder> <NSFW_media_list.csv>")
        sys.exit(1)
        
    ud_file_out = sys.argv[1]
    cd_file_out = sys.argv[2]
    ud_file = sys.argv[3]
    cd_file = sys.argv[4]
    media_folder = sys.argv[5]
    nsfw_file = sys.argv[6]
    
    delete_broken_and_nsfw(media_folder,nsfw_file)
    
    users_df = pd.read_csv(ud_file)
    content_df = pd.read_csv(cd_file)
    
    users_df, content_df = fix(users_df,content_df,media_folder)
    total,mod, images = clean_media(media_folder, blur=True)
    
    
    with open("images_with_faces.csv", 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["content_id","image"])
        writer.writeheader()
        for exp in images:
            writer.writerow(exp)
    
    users_df.to_csv(ud_file_out,index=False)
    content_df.to_csv(cd_file_out,index=False)
    
    