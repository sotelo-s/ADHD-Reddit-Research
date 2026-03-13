'''
    Une dos salidas diferentes de crawler.py.
    
    **IMPORTANTE**: las ejecuciones de las dos salidas debieron usar 
    la misma REDDIT_SECRET_KEY
    
    Además, prevalecerá "first_found_in" del fichero 1 en lugar del fichero 2
    
    Uso: python join_files.py <user_data_out.csv> <content_data_out.csv> <user_data_1.csv> <content_data_1.csv> <user_data_2.csv> <content_data_2.csv>
'''

import sys
import csv
import shutil
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawler.crawler import read_csv_file

def join_files(ud_filename_0,c_filename_0,ud_filename_1,c_filename_1,ud_filename_2,c_filename_2):
    #1. Uno usuarios
    join_user_data(ud_filename_0,ud_filename_1,ud_filename_2)
    
    #2. Uno contenido
    join_content_data(c_filename_0,c_filename_1,c_filename_2)
    
    
def join_user_data(ud_filename_0,ud_filename_1,ud_filename_2):
    columns = ["id","has_ADHD","first_found_in"]
        
    #abro los ficheros 1 y 2 como diccionarios 
    user_1 = read_csv_file(ud_filename_1)
    user_2 = read_csv_file(ud_filename_2)
    
    for user in user_2.values():
        if user["id"] not in user_1.keys():
            user_1[user["id"]] = user
        
        else:
            user_1[user["id"]]["has_ADHD"] = user_1[user["id"]]["has_ADHD"] or user["has_ADHD"]
            
            
    with open(ud_filename_0, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(user_1.values())
            
     
def join_content_data(c_filename_0,c_filename_1,c_filename_2):
    columns = ["id","user","type","subreddit","text"]
    
    #abro los ficheros 1 y 2 como diccionarios 
    content_1 = read_csv_file(c_filename_1)
    content_2 = read_csv_file(c_filename_2)
    
    shutil.copyfile(c_filename_1,c_filename_0)
    
    with open(c_filename_0, 'a', newline='', encoding='utf-8') as f:
        f.write("\n")
        w = csv.DictWriter(f, fieldnames=columns)
        
        for c in content_2.values():
            if c["id"] not in content_1.keys():
                w.writerow(c)

##-------------------CODIGO PRINCIPAL-------------------##

if __name__ == "__main__":
    
    if len(sys.argv) < 7:
        print("Uso: python join_files.py <user_data_out.csv> <content_data_out.csv> <user_data_1.csv> <content_data_1.csv> <user_data_2.csv> <content_data_2.csv>")
        sys.exit(1)
        
    ud_filename_0 = sys.argv[1]
    c_filename_0 = sys.argv[2]

    ud_filename_1 = sys.argv[3]
    c_filename_1 = sys.argv[4]
    
    ud_filename_2 = sys.argv[5]
    c_filename_2 = sys.argv[6]
   
    join_files(ud_filename_0,c_filename_0,ud_filename_1,c_filename_1,ud_filename_2,c_filename_2)
    
    
