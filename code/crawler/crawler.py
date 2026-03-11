'''
    Crawler para Old Reddit. Genera (o actualiza) dos ficheros, uno con los datos de los usuarios
    de los que se ha obtenido información y los posts y comentarios asociados a este.
    
    Uso: python crawler.py <user_data.csv> <content_data.csv> <searched_phrases.csv> <subreddit_list.json>
    Ejemplo: python3 crawler.py ./out/user_data.csv ./out/post_data.csv ./data/adhd_phrases.json ./data/adhd_search_subreddits.json
'''


import requests
from bs4 import BeautifulSoup as bs
import re
import csv
import sys
import json
import hmac
import hashlib
import os

##---------------------CONSTANTES---------------------##

#numero maximo de paginas a scrapear
MAX_SR_PAGES = 1
MAX_COMMENT_PAGES = 1
MAX_POST_PAGES = 1

#numero de elementos por pagina con un máximo de 100 (cambiar para pruebas)
LIMIT_SR = 1
LIMIT_COMMENT = 1
LIMIT_POST = 1

#El número máximo de contenido que se saca de un usuario será:
# LIMIT x (MAX_POST_PAGES + MAX_COMMENT_PAGES)
# (Nota: si se vuelve a encontrar el usuario y ha publicado más posts/comentarios desde entonces,
# este número puede ser mayor)

#url principal
URL = "https://old.reddit.com/"

#cabeceras peticion (permite perfiles/posts +18, eliminar "Cookie" para no permitirlo)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Cookie': 'over18=1'
}

##----------------VARIABLES GLOBALES-------------------##

#datos de usuarios
user_data = {}

#datos de posts y comentarios
content_data = {}

#frases que indican si el usuario tiene TDAH (cargado de un fichero)
adhd_phrases = None

#subreddits de busqueda (cargado de un fichero)
subreddits = None

#nombre de los ficheros
ud_filename = None
c_filename = None

#ficheros y writers
content_file = None
content_file_writer = None
user_file = None
user_file_writer = None

#clave secreta para el hash
secret_key = None


##---------------------FUNCIONES------------------------##

'''
    Obtiene datos de gente con y sin TDAH de una lista de subreddits.
'''
def search_users():
    for sr in subreddits:
        new_users = 0
        new_posts = 0
        new_comments = 0
        
        print(f"\n\n---- SUBREDDIT: {sr}")
        
        
        url = f"{URL}{sr}?limit={LIMIT_SR}"
        page = 1
        
        
        while url:
            response = requests.get(url,headers=HEADERS)
            
            if response.status_code == 200: #si la request funciona
        
                content = response.content
                soup = bs(content, 'html.parser')
                users = soup.find_all('div', class_='thing')
                
                #bucle de usuarios
                for user in users:
                    
                    if not user.has_attr('data-author'): #no tiene autor, cuenta eliminada, nos lo saltamos
                        continue
                    
                    #si es un post con pin (moderadores/threads) nos lo saltamos
                    if bool(user.find('span', class_='stickied-tagline')):
                        continue
                    
                    usercode = get_user_code(user,secret_key=secret_key)
                        
                    new_users_au,new_posts_au,new_comments_au = process_user_data(usercode,user)
                    new_users+=new_users_au
                    new_posts+=new_posts_au
                    new_comments+=new_comments_au
                    
                page += 1
                next_button = soup.find('span', class_='next-button')
        
                if page > MAX_SR_PAGES or not next_button or not next_button.find('a'):
                    break
        
        
                url = next_button.find('a')['href']
        
        print(f"Usuarios encontrados: {new_users}")
        print(f"Posts encontrados: {new_posts}")
        print(f"Comentarios encontrados: {new_comments}")
                        
                    
'''
    Procesa los datos de un usuario encontrado en un subreddit. 
    Devuelve si se ha generado fila de usuario (1) o no (0) el número de posts y el
    número de comentarios guardados
'''
def process_user_data(usercode,raw_user_data):
    global ud_filename
    
    new_posts = 0
    new_comments = 0
    
    username = get_username(raw_user_data)
    
    has_ADHD = False
        
    #---posts
    url = f"{URL}/user/{username}/submitted?limit={LIMIT_POST}"
    page = 1
    
    while url:
        
        response = requests.get(url,headers=HEADERS)
    
        if response.status_code == 200: #si la request funciona
            content = response.content
            soup = bs(content, 'html.parser')
            posts = soup.find_all('div', class_='thing')
                        
            for post in posts:
                code = get_content_code(post,secret_key=secret_key)
                if code in content_data:
                    continue
                adhd,n = generate_content(usercode,code,get_content_url(post), post,"post")
                new_posts+=n
                has_ADHD = has_ADHD or adhd
                
                
            page += 1
            next_button = soup.find('span', class_='next-button')
        
            if page > MAX_POST_PAGES or not next_button or not next_button.find('a'):
                break
        
            url = next_button.find('a')['href']
            
    
    #---comentarios    
    url = f"{URL}/user/{username}/comments?limit={LIMIT_COMMENT}"
    page = 1
    
    while url:
        response = requests.get(url,headers=HEADERS)
        
        if response.status_code == 200: #si la request funciona
            content = response.content
            soup = bs(content, 'html.parser')
            comments = soup.find_all('div', class_='thing')
                        
            for comment in comments:
                code = get_content_code(comment,secret_key=secret_key)
                if code in content_data:
                    continue
                
                adhd,n = generate_content(usercode,code,get_content_url(comment),comment,"comment")    
                new_comments+=n
                has_ADHD = has_ADHD or adhd
                
            page += 1
            next_button = soup.find('span', class_='next-button')
        
            if page > MAX_COMMENT_PAGES or not next_button or not next_button.find('a'):
                break
        
            url = next_button.find('a')['href']
            
    #si el usuario existia y descubrimos que tiene TDAH, lo actualizamos en fichero
    if usercode in user_data and not user_data[usercode]["has_ADHD"] and has_ADHD:
        update_user_adhd(usercode,ud_filename)
    
    #si no teniamos a ese usuario y tiene datos, creamos una entrada
    if new_comments + new_posts > 0 and usercode not in user_data:
        generate_user(usercode,raw_user_data, has_ADHD)
        return 1, new_posts, new_comments
    else:
        return 0, new_posts, new_comments
    
'''
    Crea un usuario nuevo en memoria y fichero
'''
def generate_user(usercode,raw_user_data, has_ADHD):
    global user_data
    
    data = {
        "id": usercode,
        "has_ADHD" : has_ADHD,
        "first_found_in": raw_user_data.get("data-subreddit-prefixed")
    }
    
    user_data[usercode] = data 
    append_to_file(data,"user")

'''
    Genera los datos de un post o comentario y devuelve si ha detectado que el autor tiene TDAH
    También devuelve si se ha generado fila (1) o no (0)
'''
def generate_content(usercode,content_code,url,raw_data,type):
    global content_data
    
    if content_code in content_data:
        return False, 0
    
    data = {
        "id" : content_code,
        "user" : usercode,
        "type" : type,
        "subreddit" : raw_data.get("data-subreddit-prefixed")
    } #por ahora no guardo el titulo
        
    response = requests.get(f"{URL}/{url}",headers=HEADERS)
    
    if response.status_code == 200: #si no hay errores
        soup = bs(response.content, 'html.parser')
        content = soup.find('div', {'data-permalink': url})
        
        if content is None:
            return False, 0
        
        body = content.find("div", class_="usertext-body")
        
        if body is None:
            return False, 0
        
        text = " ".join(x.get_text() for x in body.find_all(recursive=False) if x.text)
        text = re.sub(r"\([0-9]+([FM]|NB)\)", "", text) #elimino texto en el que se indica edad y genero
        text = text.replace("\t"," ").replace("\n"," ")
        text = " ".join(text.split())
        data["text"] = text.strip()
        
        if not data.get("text"):
            return False, 0
        
        if data["text"] == "[removed]": #se ha eliminado
            return False, 0
        
    else:
        return False, 0

    content_data[content_code] = data
    append_to_file(data,"content")
    
    search = ""
    if type == "post": #busco tambien en el titulo
        title = raw_data.find("a",class_="title")
        if title is None:
            title = ""
        else:
            title = title.get_text()
        search = f"{title} {data['text']}"
    else:
        search = data["text"]
        
    return search_ADHD(search), 1


'''
    Genera un código id del usuario
'''
def get_user_code(user, secret_key=None): 
    username = user.get('data-author')
    if not username:
        return None
    
    if secret_key is None:
        secret_key = os.environ.get('REDDIT_SECRET_KEY')
        if not secret_key:
            raise ValueError("Se requiere REDDIT_SECRET_KEY como variable de entorno")
        
    #crea HMAC
    hmac_obj = hmac.new(
        secret_key.encode(),
        username.encode(),
        hashlib.sha256
    )
    
    return hmac_obj.hexdigest()[:16]

'''
    Devuelve username del usuario
'''
def get_username(user): 
    return user.get('data-author')

'''
    Devuelve un código id del post/comentario
'''
def get_content_code(content,secret_key=None): 
    id =  content.get("data-fullname")
    if not id:
        return None
    
    if secret_key is None:
        secret_key = os.environ.get('REDDIT_SECRET_KEY')
        
        if not secret_key:
            raise ValueError("Se requiere REDDIT_SECRET_KEY como variable de entorno")
        
    
    hmac_obj = hmac.new(
        secret_key.encode(),
        id.encode(),
        hashlib.sha256
    )
    
    return hmac_obj.hexdigest()[:16]


'''
    Devuelve el permalink del post/comentario
'''
def get_content_url(content):
    return content.get("data-permalink")


'''
    Devuelve si el usuario indica que tiene TDAH en el texto dado
'''
def search_ADHD(text):
    return any(pattern.search(text) for pattern in adhd_phrases)


##FUNCIONES DE FICHEROS

'''
    Actualiza en el fichero que el usuario tiene TDAH
'''
def update_user_adhd(usercode,filename):
    global user_data
    global user_file, user_file_writer
    
    user_file.flush()
    user_file.close()
    
    rows = []
    updated = False
    
    with open(filename, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row['id'] == usercode and row['has_ADHD'] == "False":
                row['has_ADHD'] = "True"
                updated = True
            rows.append(row)
    
    if updated:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        #actualiza en memoria
        if usercode in user_data:
            user_data[usercode]['has_ADHD'] = True
    
    #reabre el fichero
    user_file, user_file_writer = prepare_file(filename, "user")


'''
    Lee un fichero csv y lo almacena en un diccionario donde la clave es la columna dada
'''
def read_csv_file(filename, id_column="id"):
    data = {}
    
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            dictReader = csv.DictReader(f)
            
            #por cada fila del csv
            for row in dictReader:
                if "has_ADHD" in row: #convierto columna a boolean
                    row["has_ADHD"] = row["has_ADHD"].lower() == "true"
                    
                data[row[id_column]] = row
                
    except FileNotFoundError:
        pass
            
    return data

'''
    Lee un fichero JSON
'''
def read_json_file(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
    
    return data

'''
    Abre o crea el fichero de usuario/contendio
'''
def prepare_file(filename,type):
    if type == "user":
        columns = ["id","has_ADHD","first_found_in"]
    else:
        columns = ["id","user","type","subreddit","text"]
        
    file_exists = False
            
    try: #prueba si el fichero existe o no
        with open(filename, 'r', encoding='utf-8') as f:
            file_exists = f.read(1) != ''
    except FileNotFoundError:
        file_exists = False
        
    
    #abrimos fichero
    f = open(filename, 'a', newline='', encoding='utf-8')
    w = csv.DictWriter(f, fieldnames=columns)
    
    if not file_exists: #si no existia, escribimos cabecera
        w.writeheader()
        f.flush()
                
    return f, w

'''
    Añade una nueva fila al csv (mediante un diccionario)
'''
def append_to_file(dictRow,type):    
    if type == "user":
        user_file_writer.writerow(dictRow)
    else:
        content_file_writer.writerow(dictRow)
        
'''
    Procesa las frases dadas para que permita palabras comodín ("?") y variaciones de "ADHD"
'''        
def process_phrases():
    adhd_phrases = []
    for phrase in raw_phrases:
        phrase = phrase.lower()
        
        adhd_variations = [
            r'ADHD',
            r'ADHDau', 
            r'ADD[/ ]?ADHD',
            r'ADD\s+ADHD',
            r'ADD/?ADHD'
        ]
        adhd_pattern = r'(?:' + '|'.join(adhd_variations) + r')'
        
        temp_phrase = phrase.replace('adhd', '{{ADHD}}')
            
        pattern = temp_phrase.casefold()
            
        pattern = pattern.replace('?', r'\w+')
            
        pattern = pattern.replace('{{adhd}}', adhd_pattern)
            
        pattern = r'\b' + pattern + r'\b'
            
        adhd_phrases.append(re.compile(pattern, re.IGNORECASE))

    return adhd_phrases


##-------------------CODIGO PRINCIPAL-------------------##

if len(sys.argv) < 5:
    print("Uso: python crawler.py <user_data> <content_data> <searched_phrases.csv> <subreddit_list.json>")
    sys.exit(1)
    
ud_filename = sys.argv[1]
c_filename = sys.argv[2]
phrases_filename = sys.argv[3]
sr_filename = sys.argv[4]

#leo los ficheros
user_data = read_csv_file(ud_filename)
content_data = read_csv_file(c_filename)

raw_phrases = read_json_file(phrases_filename)
subreddits = read_json_file(sr_filename)


#preparo los ficheros para añadir informacion
user_file,user_file_writer = prepare_file(ud_filename,"user")
content_file,content_file_writer = prepare_file(c_filename,"content")

#preparo las frases
adhd_phrases = process_phrases()

#obtengo la clave secreta para el hash
secret_key = os.environ.get('REDDIT_SECRET_KEY')

#busco usuarios y contenido
search_users()

#cerramos ficheros
user_file.close()
content_file.close()