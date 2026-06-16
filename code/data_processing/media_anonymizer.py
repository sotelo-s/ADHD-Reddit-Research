'''
    Applies Gaussian blur to people faces
    
    Use: python media_anonymizer.py <media_folder> [blur]
'''

import sys
from huggingface_hub import hf_hub_download
from ultralytics import YOLO
from supervision import Detections
from PIL import Image, ImageFilter
import os
import numpy as np
import csv

CSV_file = "images_with_faces.csv"
FIELD_NAMES = [
    "content_id",
    "image"
]

def clean_media(media_folder, blur=False):
    model_path = hf_hub_download(repo_id="arnabdhar/YOLOv8-Face-Detection", filename="model.pt")
    model = YOLO(model_path)

    folders = os.listdir(media_folder)
    
    total_images = 0
    mod_images = 0
    
    images_with_faces = []
    
    for c_id in folders:
        for image in os.listdir(f"{media_folder}/{c_id}"):
            output = model(Image.open(f"{media_folder}/{c_id}/{image}"))
            
            try:
                original_image = Image.open(f"{media_folder}/{c_id}/{image}")
                total_images += 1
                
                output = model(original_image)
                results = Detections.from_ultralytics(output[0])
                
                if len(results.xyxy) > 0:  #se detecta cara
                    if blur:
                        blurred_image = blur_face(original_image, results.xyxy)
                        
                        #blurred_image.save(f"{media_folder}/{c_id}/blurred_{image}")
                        blurred_image.save(f"{media_folder}/{c_id}/{image}")
                        
                    images_with_faces.append({
                        "content_id" : c_id,
                        "image" : image
                    })
                    
                    mod_images += 1
                    
            except Exception as e:
                print(f"Error processing {image}: {str(e)}")
    
    return total_images, mod_images, images_with_faces
            
def blur_face(image, boxes, blur_radius=30):
    img_array = np.array(image)
    
    for box in boxes:
        x1, y1, x2, y2 = map(int, box)
        
        #coordenadas dentro de la imagen
        x1 = max(0, x1)
        y1 = max(0, y1)
        x2 = min(img_array.shape[1], x2)
        y2 = min(img_array.shape[0], y2)
        
        #donde esta la cara
        face_region = img_array[y1:y2, x1:x2]
        
        #blur gaussiano
        if face_region.size > 0:
            face_pil = Image.fromarray(face_region)
            blurred_face = face_pil.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            img_array[y1:y2, x1:x2] = np.array(blurred_face)
    
    return Image.fromarray(img_array)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Use: python media_anonymizer.py <media_folder> [blur]")
        sys.exit(1)
        
    media_folder = sys.argv[1]
    
    blur = True if len(sys.argv) > 2 and sys.argv[2] == "blur" else False
    
    total,mod, images = clean_media(media_folder, blur)
    
    print(f"Total images: {total}")
    print(f"Images with faces: {mod}")
    
    with open(CSV_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=FIELD_NAMES)
        writer.writeheader()
        for exp in images:
            writer.writerow(exp)
    
