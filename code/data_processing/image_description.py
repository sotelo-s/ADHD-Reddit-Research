'''
Adds "image_description" column with a textual description of the image(s), if apply.
'''

from transformers import BlipProcessor, BlipForConditionalGeneration
import torch
from PIL import Image
import pandas as pd
import torch
from pathlib import Path


in_file = "../../in/content.csv"
out_file = "../../out/content_with_image_descriptions.csv"
images_folder = "../../in/media"

processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base", torch_dtype=torch.float16).to("cuda")


content_df = pd.read_csv(in_file)


def add_descriptions(row):
    if not row["has_media"]:
        row["image_description"] = ""
        return row

    images = None
    content_path = f"{images_folder}/{row.id}"
    path_obj = Path(content_path)

    if  path_obj.exists():
        images = [str(image.resolve()) for image in list(path_obj.iterdir())]
    else:
        images = None
        
    open_images = []
    if images:
        for path in images:
            try:
                image = Image.open(path).convert("RGB")
                #image.thumbnail((128, 128)) #resize
                open_images.append(image)
                
            except Exception as e:
                ...
        
    if not open_images:
        row["image_description"] = ""
        return row

    descriptions = []
    for image in open_images:
        inputs = processor(image, return_tensors="pt").to("cuda", torch.float16)
        out = model.generate(**inputs)
        description = processor.decode(out[0], skip_special_tokens=True)
        descriptions.append(description)

    row["image_description"] = " ".join(descriptions)
    return row
    

content_df["text"] = content_df["text"].astype(str).fillna("")
content_df = content_df.apply(add_descriptions,axis=1)


content_df.to_csv(out_file,index=False)
