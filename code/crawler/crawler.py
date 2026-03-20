'''
    Crawler para Old Reddit. Genera (o actualiza) dos ficheros, uno con los datos de los usuarios
    de los que se ha obtenido información y los posts y comentarios asociados a este.
    
    Uso: python crawler.py <user_data.csv> <content_data.csv> <searched_phrases.csv> <subreddit_list.json>
    Ejemplo: python3 crawler.py ../out/user_data.csv ../out/post_data.csv ./data/adhd_phrases.json ./data/adhd_search_subreddits.json
'''


import requests
import re
import csv
import sys
import json
import hmac
import hashlib
import os
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from datetime import datetime,timezone



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
#no permite visitar otros subreddits que tienen otro tipo de warnings (ej: r/Drugs)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Cookie': 'over18=1'
}

#variación en los delays en segundos
RANDOM_DELAY_VARIATION = 1.5

#delay base antes de hacer peticiones
BASE_DELAY = 5

MAX_RETRIES = 3

INITIAL_TIMEOUT = 30

##----------------VARIABLES GLOBALES-------------------##

#datos de usuarios
user_data = {}

#datos de posts y comentarios
content_data = {}

#frases que indican si el usuario tiene TDAH (cargado de un fichero)
adhd_pattern = None

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

session = None

file_lock = threading.Lock()


##---------------------FUNCIONES------------------------##


def search_users():
    '''
        Obtiene datos de gente con y sin TDAH de una lista de subreddits.
    '''
    for sr in subreddits:
        new_users = 0
        new_posts = 0
        new_comments = 0
        
        print(f"\n\n---- SUBREDDIT: {sr}")
        
        
        url = f"{URL}{sr}/.json?limit={LIMIT_SR}"
        page = 1
        
        
        
        while url:
            retries = 0
            success = False
            
            while retries < MAX_RETRIES and not success:
                try:
                    response = safe_get(url)
                    
                    if response.status_code == 429:
                        retries+=1
                        if retries < MAX_RETRIES:
                            print(f"Rate limit alcanzado para url: {url}.\nIntento {retries}/{MAX_RETRIES}. Esperando {INITIAL_TIMEOUT*retries}s...")
                            random_delay(INITIAL_TIMEOUT*retries)
                            continue
                        else:
                            print("Número máximo de intentos alcanzado.")
                            break
                        
                    if response.status_code == 200: #si la request funciona
                        
                        
                        try:
                            content = response.json().get("data", {})
                        except ValueError:
                            continue
                        
                        success = True
                        
                        users = content.get('children', [])
                                                
                        #bucle de usuarios
                        for u in users:
                            
                            user = u.get("data",{})
                            
                            if user.get('author','') == "[deleted]" or user.get('author','').lower() == "automoderator": #cuenta eliminada o automod, nos lo saltamos
                                continue
                            
                            #si es un post de mod nos lo saltamos
                            if user.get('distinguished') == "moderator":
                                continue
                            
                            usercode = get_user_code(user,secret_key=secret_key)
                            if not usercode:
                                continue
                                
                            new_users_au,new_posts_au,new_comments_au = process_user_data(usercode,user)
                            new_users+=new_users_au
                            new_posts+=new_posts_au
                            new_comments+=new_comments_au
                            

                                            
                except (requests.ConnectionError, requests.Timeout) as e:
                    print(f"Error de conexión: {e}")
                    print("Esperando 60 segundos antes de reintentar...")
                    random_delay(60)
                    continue
                
            if not success:
                break
            
            page += 1
            after = content.get("after")    
            count = content.get("dist",LIMIT_SR)
    
            if page > MAX_SR_PAGES or not after:
                url = None
            else:
                url = f"{URL}{sr}/.json?limit={LIMIT_SR}&after={after}&count={(page-1)*count}"
            
        
        print(f"Usuarios encontrados: {new_users}")
        print(f"Posts encontrados: {new_posts}")
        print(f"Comentarios encontrados: {new_comments}")
                        
                    
def process_user_data(usercode,raw_user_data):
    '''
    Procesa los datos de un usuario encontrado en un subreddit. 
    Devuelve si se ha generado fila de usuario (1) o no (0) el número de posts y el
    número de comentarios guardados
    '''
    global ud_filename
    
    new_posts = 0
    new_comments = 0
    
    username = get_username(raw_user_data)
    
    if username == "[deleted]":
        return 0,0,0
    
    has_ADHD = False
        
    #---posts
    url = f"{URL}user/{username}/submitted/.json?limit={LIMIT_POST}"
    page = 1
    
    while url:
        retries = 0
        success = False
        
        while retries < MAX_RETRIES and not success:
            try:
                response = safe_get(url)
                
                if response.status_code == 403 or response.status_code == 404: #ej. cuenta suspendida
                    return 0,0,0
                
                if response.status_code == 429:
                    retries+=1
                    if retries < MAX_RETRIES:
                        print(f"Rate limit alcanzado para url: {url}.\nIntento {retries}/{MAX_RETRIES}. Esperando {INITIAL_TIMEOUT*retries}s...")
                        random_delay(INITIAL_TIMEOUT*retries)
                        continue
                    else:
                        print("Número máximo de intentos alcanzado.")
                        break
        
                if response.status_code == 200: #si la request funciona
                    
                    
                    try:
                        content = response.json().get("data",{})
                    except ValueError:
                        break
                    
                    success = True
                    
                    
                    posts = content.get('children', [])
                                                
                    for p in posts:
                        post = p.get("data",{})
                        
                        if post.get('distinguished') == "moderator":
                            continue
                        
                        code = get_content_code(post,secret_key=secret_key)
                        if not code or code in content_data:
                            continue
                        adhd,n = generate_content(usercode,code, post,"post")
                        new_posts+=n
                        has_ADHD = has_ADHD or adhd
                                            
                        
                    
                                    
            except (requests.ConnectionError, requests.Timeout) as e:
                print(f"Error de conexión: {e}")
                print("Esperando 60 segundos antes de reintentar...")
                random_delay(60)
                continue
            
            if not success:
                break
            
            page += 1
            after = content.get("after")
            count = content.get("dist",LIMIT_POST)
        
            if page > MAX_POST_PAGES or not after:
                url = None
            else:
                url = f"{URL}user/{username}/submitted/.json?limit={LIMIT_POST}&after={after}&count={(page-1)*count}"
    
    #---comentarios    
    url = f"{URL}user/{username}/comments/.json?limit={LIMIT_COMMENT}"
    page = 1
    
    while url:
        retries = 0
        success = False
        
        while retries < MAX_RETRIES and not success:
            try:
                response = safe_get(url)
                
                if response.status_code == 403 or response.status_code == 404: #ej. cuenta suspendida
                    return 0,0,0
                
                if response.status_code == 429:
                    retries+=1
                    if retries < MAX_RETRIES:
                        print(f"Rate limit alcanzado para url: {url}.\nIntento {retries}/{MAX_RETRIES}. Esperando {INITIAL_TIMEOUT*retries}s...")
                        random_delay(INITIAL_TIMEOUT*retries)
                        continue
                    else:
                        print("Número máximo de intentos alcanzado.")
                        break
            
                if response.status_code == 200: #si la request funciona
                    
                    try:
                        content = response.json().get("data",{})
                    except ValueError:
                        break
                    
                    success = True
                    
                    comments = content.get('children', [])
                    
                                       
                    for c in comments:
                        comment = c.get("data",{})
                        
                        if comment.get('distinguished') == "moderator":
                            continue
                        
                        code = get_content_code(comment,secret_key=secret_key)
                        if not code or code in content_data:
                            continue
                        
                        adhd,n = generate_content(usercode,code,comment,"comment")    
                        new_comments+=n
                        has_ADHD = has_ADHD or adhd
                        
                        

                                    
                                    
            except (requests.ConnectionError, requests.Timeout) as e:
                print(f"Error de conexión: {e}")
                print("Esperando 60 segundos antes de reintentar...")
                random_delay(60)
                continue
            
            if not success:
                break
            
            page += 1
            after = content.get("after")
            count = content.get("dist",LIMIT_COMMENT)
                        
            if page > MAX_COMMENT_PAGES or not after:
                url = None
            else:
                url = f"{URL}user/{username}/comments/.json?limit={LIMIT_COMMENT}&after={after}&count={(page-1)*count}"
            
    #si el usuario existia y descubrimos que tiene TDAH, lo actualizamos en fichero
    if usercode in user_data and not user_data[usercode]["has_ADHD"] and has_ADHD:
        update_user_adhd(usercode,ud_filename)
    
    #si no teniamos a ese usuario y tiene datos, creamos una entrada
    if new_comments + new_posts > 0 and usercode not in user_data:
        generate_user(usercode,raw_user_data, has_ADHD)
        return 1, new_posts, new_comments
    else:
        return 0, new_posts, new_comments
    

def generate_user(usercode,raw_user_data, has_ADHD):
    '''
    Crea un usuario nuevo en memoria y fichero
    '''
    global user_data
    
    data = {
        "id": usercode,
        "has_ADHD" : has_ADHD,
        "first_found_in": raw_user_data.get("subreddit_name_prefixed")
    }
    
    user_data[usercode] = data 
    append_to_file(data,"user")


def generate_content(usercode,content_code,raw_data,type):
    '''
    Genera los datos de un post o comentario y devuelve si ha detectado que el autor tiene TDAH
    También devuelve si se ha generado fila (1) o no (0)
    '''
    global content_data
    
    if not raw_data or raw_data == {}:
        return False,0
    
    if content_code in content_data:
        return False, 0
    
    data = {
        "id" : content_code,
        "user" : usercode,
        "type" : type,
        "subreddit" : raw_data.get("subreddit_name_prefixed")
    } #por ahora no guardo el titulo
    
    timestamp_utc = raw_data.get("created_utc")
    created_date = ""
    if timestamp_utc:
        created_date = datetime.fromtimestamp(timestamp_utc, timezone.utc)

    data["timestamp"] = created_date
    
    if type == "post":
        body_search = "selftext"
    else:
        body_search = "body"
        
    body = raw_data.get(body_search)
    
    if body is None:
        return False, 0
    
    text = body.replace("\t"," ").replace("\n"," ")
    text = " ".join(text.split())
    data["text"] = text.strip()
    
    if not data.get("text"):
        return False, 0
    
    if data["text"] == "[removed]" or data["text"] == "" or data["text"] == "[deleted]" or data["text"] == "<image>":
        return False, 0
        
        
    search = ""
    if type == "post": #busco tambien en el titulo
        title = raw_data.get("title","")
        search = f"{title} {data['text']}"
    else:
        search = data["text"]
        
    adhd = search_ADHD(adhd_pattern,search)
    data["has_ADHD_pattern"] = adhd

    content_data[content_code] = data
    append_to_file(data,"content")
    
        
    return adhd, 1



def get_user_code(user, secret_key=None): 
    '''
    Genera un código id del usuario
    '''
    username = user.get('author')
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


def get_username(user): 
    '''
    Devuelve username del usuario
    '''
    return user.get('author')


def get_content_code(content,secret_key=None): 
    '''
    Devuelve un código id del post/comentario
    '''
    id =  content.get("id")
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



def search_ADHD(adhd_pattern,text):
    '''
    Devuelve si el usuario indica que tiene TDAH en el texto dado
    '''
    return bool(adhd_pattern.search(text))


##FUNCIONES DE FICHEROS


def update_user_adhd(usercode,filename):
    '''
    Actualiza en el fichero que el usuario tiene TDAH
    '''
    global user_data
    global user_file, user_file_writer
    
    with file_lock:
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



def read_csv_file(filename, id_column="id"):
    '''
    Lee un fichero csv y lo almacena en un diccionario donde la clave es la columna dada
    '''
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


def read_json_file(filename):
    '''
    Lee un fichero JSON
    '''
    with open(filename, 'r') as f:
        data = json.load(f)
    
    return data

def prepare_file(filename,type):
    '''
        Abre o crea el fichero de usuario/contendio
    '''
    if type == "user":
        columns = ["id","has_ADHD","first_found_in"]
    else:
        columns = ["id","user","type","subreddit","text","timestamp","has_ADHD_pattern"]
        
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


def append_to_file(dictRow,type):  
    '''
    Añade una nueva fila al csv (mediante un diccionario)
    '''  
    with file_lock:
        if type == "user":
            user_file_writer.writerow(dictRow)
        else:
            content_file_writer.writerow(dictRow)


def flush_files():
    '''
    Función para forzar el vaciado de los buffers de archivos
    '''

    with file_lock:
        try:
            if user_file:
                user_file.flush()
                os.fsync(user_file.fileno())
            if content_file:
                content_file.flush()
                os.fsync(content_file.fileno())
        except Exception as e:
            print(f"Error al escribir ficheros: {e}")
 
       
def process_phrases(raw_phrases):
    '''
    Procesa las frases dadas para que permita palabras comodín ("?") y variaciones de "ADHD"
    ''' 
    adhd_phrases = []
    
    adhd_pattern = r'(?:ADHDau|AuDHD|ADHD|ADD[ \/]?ADHD|attention deficit hyperactivity disorder)'    
    
    for phrase in raw_phrases:
        phrase = phrase.lower()
        
        phrase = phrase.replace('adhd', '{{ADHD}}')
            
        phrase = re.escape(phrase)
            
        phrase = phrase.replace(r'\?', r'[\w\'-]+')
            
        phrase = phrase.replace(r'\{\{ADHD\}\}', adhd_pattern)
            
        phrase = r'\b' + phrase + r'\b'
                    
        adhd_phrases.append(phrase)
        
    pattern = r'(?:' + '|'.join(adhd_phrases) + r')'
    
    return re.compile(pattern,re.IGNORECASE)


def create_session():
    '''
    Crea una sesión para hacer requests
    '''
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    
    return session


def random_delay(base_delay):
    '''
    Añade variación aleatoria al delay base
    '''
    variation = random.uniform(-RANDOM_DELAY_VARIATION, RANDOM_DELAY_VARIATION)
    actual_delay = max(0.5, base_delay + variation)
    
    #se aprovecha la pausa para guardar los ficheros
    flush_thread = threading.Thread(target=flush_files)
    flush_thread.daemon = True
    flush_thread.start()
    
    time.sleep(actual_delay)
    
    #esperamos a que termine el flush
    flush_thread.join(timeout=5)


def safe_get(url):
    '''
    Hace una request, añadiendo un delay.
    '''
    random_delay(BASE_DELAY)
    return session.get(url, timeout=30)

##-------------------CODIGO PRINCIPAL-------------------##

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python crawler.py <user_data> <content_data> <searched_phrases.csv> <subreddit_list.json>")
        sys.exit(1)
        
    ud_filename = sys.argv[1]
    c_filename = sys.argv[2]
    phrases_filename = sys.argv[3]
    sr_filename = sys.argv[4]
    
    #obtengo la clave secreta para el hash
    secret_key = os.environ.get('REDDIT_SECRET_KEY')
    
    if not secret_key:
        raise ValueError("Se requiere REDDIT_SECRET_KEY como variable de entorno")

    #leo los ficheros
    user_data = read_csv_file(ud_filename)
    content_data = read_csv_file(c_filename)

    raw_phrases = read_json_file(phrases_filename)
    subreddits = read_json_file(sr_filename)


    #preparo los ficheros para añadir informacion
    user_file,user_file_writer = prepare_file(ud_filename,"user")
    content_file,content_file_writer = prepare_file(c_filename,"content")

    #preparo las frases
    adhd_pattern = process_phrases(raw_phrases)

    #creo la sesión
    session = create_session()

    #busco usuarios y contenido
    try:
        search_users()
    finally:
        flush_files()

    #cerramos ficheros
    with file_lock:
        user_file.close()
        content_file.close()
    
    sys.exit(0)