'''
    Actualiza la clasificación de TDAH de los usuarios basándose en nuevas frases o criterios.
    
    También añade la columna "has_ADHD_pattern" que indica si se encuentra algún patrón de la
    lista en el comentario o post.
    
    Uso: python update_adhd.py <user_data_out.csv> <content_data_out.csv> <user_data.csv> <content_data.csv> <adhd_phrases> [-rewrite]
    
    -rewrite:   Flag opcional. Si se incluye, realiza una reevaluación completa,
                permitiendo que usuarios previamente clasificados como True puedan
                pasar a False si los nuevos criterios no los respaldan.
'''

import argparse
import sys
import csv
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawler.crawler import read_json_file,process_phrases,search_ADHD, read_csv_file

def update_adhd(user_out_file,content_out_file,user_file,content_file,phrases_file,rewrite=False):
    adhd_pattern = process_phrases(read_json_file(phrases_file))
    
    user_data = read_csv_file(user_file)
    content_data = read_csv_file(content_file)
    
    content_to_check = content_data.values()
    
    if rewrite:
        for user in user_data.keys():
            user_data[user]["has_ADHD"] = False
        
        for content in content_data.keys():
            content_data[content]["has_ADHD_pattern"] = False

    for content in content_to_check:
        has_ADHD = search_ADHD(adhd_pattern,content["text"])
        user_data[content["user"]]["has_ADHD"] = user_data[content["user"]]["has_ADHD"] or has_ADHD
        
        has_ADHD_pattern = content_data[content["id"]].get("has_ADHD_pattern", False)
        content_data[content["id"]]["has_ADHD_pattern"] = has_ADHD_pattern or has_ADHD
        
    update_users(user_data,user_out_file)
    update_content(content_data,content_out_file)
            
    

def update_users(user_data,filename):
    columns = ["id","has_ADHD","first_found_in"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(user_data.values())


def update_content(content_data,filename):
    columns = ["id","user","type","subreddit","text","has_ADHD_pattern"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(content_data.values())

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("user_data_out")
    
    parser.add_argument("content_data_out")
    
    parser.add_argument("user_file")
    
    parser.add_argument("content_file")
    
    parser.add_argument("adhd_phrases")
    
    parser.add_argument("-rewrite",action="store_true")
    
    return parser.parse_args()


##-------------------CODIGO PRINCIPAL-------------------##

if __name__ == "__main__":
    
    args = parse_arguments()
    
    if len(sys.argv) < 6:
        parser = argparse.ArgumentParser()
        parser.print_help()
        sys.exit(1)
        
    update_adhd(args.user_data_out,args.content_data_out,args.user_file,args.content_file,args.adhd_phrases,rewrite=args.rewrite)
