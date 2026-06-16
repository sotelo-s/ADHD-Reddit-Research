'''
    Censors NSFW content
    
    Use: python nsfw_censoring.py <media_folder> [threshold]
'''

import sys
from PIL import Image
import os
from transformers import pipeline
import torch
import csv

CSV_file = "nsfw_images.csv"
FIELD_NAMES = [
    "content_id",
    "image"
]

def clean_media(media_folder, threshold=0.7, censor=False):
    device = 0 if torch.cuda.is_available() else -1
    nsfw_classifier = pipeline(
        "image-classification", 
        model="Falconsai/nsfw_image_detection",
        device=device
    )
    
    folders = os.listdir(media_folder)
    
    total_images = 0
    mod_images = 0
    images = []
    
    for c_id in folders:
        folder_path = f"{media_folder}/{c_id}"
            
        for image in os.listdir(folder_path):
            try:
                original_image = Image.open(f"{folder_path}/{image}")
                total_images += 1
                
                predictions = nsfw_classifier(original_image)
                
                is_nsfw = False
                for pred in predictions:
                    if pred['label'] == 'nsfw' and pred['score'] > threshold:
                        is_nsfw = True
                        break
                
                if is_nsfw:
                    if censor:
                        width, height = original_image.size
                        
                        small_width = max(1, width // 200)
                        small_height = max(1, height // 200)
                        
                        small_image = original_image.resize((small_width, small_height), Image.NEAREST)
                        
                        pixelated_image = small_image.resize((width, height), Image.NEAREST)
                        
                        pixelated_image.save(f"{folder_path}/{image}")
                        
                    mod_images += 1
                    images.append({
                        "content_id": c_id,
                        "image": image
                    })
                    
                    
            except Exception as e:
                print(f"Error processing {folder_path}/{image}: {str(e)}")
    
    return total_images, mod_images, images

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Use: python nsfw_censoring.py <media_folder> [censor]")
        sys.exit(1)
        
    media_folder = sys.argv[1]
    censor = True if len(sys.argv) > 2 and sys.argv[2] == "censor" else False

    
    total, mod, images = clean_media(media_folder, 0.7, censor)
    
    print(f"Total images: {total}")
    print(f"NSFW images: {mod}")
    
    with open(CSV_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=FIELD_NAMES)
        writer.writeheader()
        for exp in images:
            writer.writerow(exp)