import boto3
import os
import json
#import pytz
#import concurrent.futures
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Cargar las variables desde el archivo .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
LOCAL_DOWNLOAD_PATH = os.getenv('LOCAL_DOWNLOAD_PATH')
LAST_PROCESSED_FILE = os.getenv('LAST_PROCESSED_FILE')
DAYS_TO_FILTER = int(os.getenv('DAYS_TO_FILTER'))
PREFIX= os.getenv('PREFIX')
FILE_KEY = os.getenv('FILE_KEY')
FILE_SKIP_PATH = os.getenv('FILE_SKIP_PATH')
FILE_VALID_PATH = os.getenv('FILE_VALID_PATH')

COST_PER_LIST_REQUEST = os.getenv('COST_PER_LIST_REQUEST')
COST_PER_GET_REQUEST = os.getenv('COST_PER_GET_REQUEST')
COST_PER_GB_TRANSFERRED = os.getenv('COST_PER_GB_TRANSFERRED')

# Calcula la fecha límite para el filtro 
DATE_LIMIT = datetime.now(timezone.utc) - timedelta(days=DAYS_TO_FILTER)

# Lee el archivo JSON o crea uno nuevo si no existe
if os.path.exists(FILE_SKIP_PATH):
    with open(FILE_SKIP_PATH, 'r') as f:
        data = json.load(f)
else:
    data = {
        "dateLimit": DATE_LIMIT.isoformat(),
        "skip": []
    }

if os.path.exists(FILE_VALID_PATH):
    with open(FILE_VALID_PATH, 'r') as f:
        validPath = json.load(f)
else:
    validPath = {
        "path": []
    }

# Agrega dateLimit a data si no existe
if "dateLimit" not in data:
    data["dateLimit"] = DATE_LIMIT.isoformat()
else:
    # Comparación de dateLimit con DATE_LIMIT
    dateLimit_dt = datetime.fromisoformat(data["dateLimit"])
    if DATE_LIMIT > dateLimit_dt:
        data["dateLimit"] = DATE_LIMIT.isoformat()
    else:
        DATE_LIMIT = dateLimit_dt

s3 = boto3.client( 's3', 
                      aws_access_key_id=AWS_ACCESS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY )

# Ejemplo de uso
#print (get_last_modified_folder(BUCKET_NAME, PREFIX))
#print (DATE_LIMIT)

def agregar_path_validPath(path):
    if path not in validPath["path"]:
        validPath["path"].append(path)
        print(f"La ruta {path} ha sido agregada.")
    else:
        print(f"La ruta {path} ya se encuentra en el arreglo.")
    
    with open(FILE_VALID_PATH, 'w') as f:
        json.dump(validPath, f, indent=4)

def agregar_path_skip(prefix, lastModified):
    data["skip"].append({"path": prefix, "lastModified": lastModified.isoformat()})
    print(f"La ruta '{prefix}' ha sido agregada al JSON.")

    with open(FILE_SKIP_PATH, 'w') as f:
        json.dump(data, f, indent=4)

def borrar_path_validPath(path):
    if path in validPath["path"]:
        validPath["path"].remove(path)
        print(f"La ruta {path} ha sido eliminada.")
    
    with open(FILE_VALID_PATH, 'w') as f:
        json.dump(validPath, f, indent=4)

def borrar_path_skip(prefix):
    for entry in data["skip"]:
        if entry["path"] == prefix:
            data["skip"].remove(entry)
            print(f"La ruta '{prefix}' ha sido borrada del JSON.")
            break
    
    with open(FILE_SKIP_PATH, 'w') as f:
        json.dump(data, f, indent=4)

def get_last_modified_folder_NEW(response, prefix):    
    if 'Contents' in response:
        last_modified_dates = [obj['LastModified'] for obj in response['Contents']]
        last_modified_date = max(last_modified_dates)

        if contiene_prefijo(data, prefix):
            if last_modified_date < DATE_LIMIT:
                print(f"La ruta '{prefix}' ha sido saltada.")
                return
            else:
                borrar_path_skip(prefix)
                agregar_path_validPath(prefix)
        else:
            if last_modified_date < DATE_LIMIT:
                agregar_path_skip(prefix, last_modified_date)
                return
            else:
                agregar_path_validPath(prefix)
                return
    else:
        return False

def contiene_prefijo(data, prefix):
    path_set = set(entry["path"] for entry in data["skip"])
    return any(path.startswith(prefix) for path in path_set)


def list_folders_in_path(bucket_name, prefix=""):
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/',
                ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )

        get_last_modified_folder_NEW(response, prefix)

        if 'CommonPrefixes' in response:
            for folder in response['CommonPrefixes']:
               list_folders_in_path(bucket_name, folder['Prefix'])

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
            print(continuation_token)
        else:
            break
 

# Ejemplo de uso
list_folders_in_path(BUCKET_NAME, PREFIX)
print (validPath["path"])


import boto3

def list_files_in_s3(bucket_name, prefix=''):
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        print(f"Files in '{bucket_name}/{prefix}':")
        for obj in response['Contents']:
            # Verifica si el nombre del objeto no termina en '/' (no es un prefijo)
            if not obj['Key'].endswith('/'):
                print(obj['Key'])
    else:
        print(f"No files found in '{bucket_name}/{prefix}'. Make sure the bucket name and prefix are correct.")

# Ejemplo de uso
#list_files_in_s3(BUCKET_NAME, PREFIX)


def download_file_from_s3(bucket_name, file_key, download_dir=LOCAL_DOWNLOAD_PATH):
    # Construye la ruta completa de destino
    local_path = os.path.join(download_dir, file_key)

    # Crea las carpetas necesarias para la ruta de destino
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    try:
        # Descarga el archivo
        s3.download_file(bucket_name, file_key, local_path)
        print(f"File '{file_key}' from bucket '{bucket_name}' downloaded to '{local_path}'")
    except Exception as e:
        print(f"An error occurred: {e}")

# Ejemplo de uso
#download_file_from_s3(BUCKET_NAME, FILE_KEY)

def authenticate_google_drive():
    gauth = GoogleAuth()
    # Intenta cargar credenciales existentes
    if os.path.exists("credentials.json"):
        gauth.LoadCredentialsFile("credentials.json")
        
        if gauth.credentials is None or gauth.access_token_expired:
            # Las credenciales no existen o han expirado, realiza la autenticación
            gauth.LocalWebserverAuth()
            # Guarda las credenciales actualizadas
            gauth.SaveCredentialsFile("credentials.json")
        else:
            # Las credenciales son válidas
            gauth.Authorize()
    else:
        # No hay credenciales guardadas, realiza la autenticación inicial
        gauth.LoadClientConfigFile("client_secrets.json")
        gauth.LocalWebserverAuth()
        # Guarda las credenciales para uso futuro
        gauth.SaveCredentialsFile("credentials.json")

    return GoogleDrive(gauth)

def create_folder(drive, folder_name, parent_folder_id=None):
    folder_metadata = {
        'title': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_folder_id:
        folder_metadata['parents'] = [{'id': parent_folder_id}]
    
    folder = drive.CreateFile(folder_metadata)
    folder.Upload()
    return folder['id']

def get_or_create_folder(drive, folder_name, parent_folder_id):
    # Buscar si la carpeta ya existe en la ruta especificada
    query = f"'{parent_folder_id}' in parents and title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    
    if file_list:
        return file_list[0]['id']
    else:
        return create_folder(drive, folder_name, parent_folder_id)

def upload_files_to_drive(drive, local_path, parent_folder_id=None):
    for root, dirs, files in os.walk(local_path):
        path_from_local = os.path.relpath(root, local_path)
        folders = path_from_local.split(os.sep)
        
        # Crear carpetas intermedias en Google Drive
        current_folder_id = parent_folder_id
        for folder in folders:
            if folder != '.':
                current_folder_id = get_or_create_folder(drive, folder, current_folder_id)
        
        # Subir archivos a la carpeta correcta en Google Drive
        for file in files:
            file_path = os.path.join(root, file)
            file_drive = drive.CreateFile({'title': file, 'parents': [{'id': current_folder_id}]})
            file_drive.SetContentFile(file_path)
            file_drive.Upload()
            print(f'File "{file_path}" uploaded to Google Drive.')

def main():
    drive = authenticate_google_drive()
    
    # Id de la carpeta raíz en Google Drive (cambiar según sea necesario)
    uploads_folder_id = 'root'  # Cambiar por el ID de tu carpeta específica si es necesario
    
    # Crear o obtener la carpeta 'uploads' en Google Drive
    uploads_folder_id = get_or_create_folder(drive, 'uploads', uploads_folder_id)
    
    # Ruta local de la carpeta de descargas
    local_downloads_path = LOCAL_DOWNLOAD_PATH
    
    upload_files_to_drive(drive, local_downloads_path, uploads_folder_id)

#if __name__ == '__main__':
#    main()

# Espera a que el usuario presione Enter para finalizar
#input("Presiona Enter para finalizar...")

# Cierra el archivo JSON al final del script (sin guardar)
with open(FILE_SKIP_PATH, 'r'):
    pass
#with open(FILE_VALID_PATH, 'r'):
#    pass