import boto3
import os
import json
import concurrent.futures
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import copy
import shutil
import stat

# Load environment variables from .env file
load_dotenv()

# Environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
LOCAL_DOWNLOAD_PATH = os.getenv('LOCAL_DOWNLOAD_PATH')
LAST_PROCESSED_FILE = os.getenv('LAST_PROCESSED_FILE')
DAYS_TO_FILTER = int(os.getenv('DAYS_TO_FILTER'))
PREFIX = os.getenv('PREFIX')
FILE_KEY = os.getenv('FILE_KEY')
FILE_SKIP_PATH = os.getenv('FILE_SKIP_PATH')
FILE_VALID_PATH = os.getenv('FILE_VALID_PATH')
COSTS_FILE_PATH = os.getenv('COSTS_FILE_PATH')
FILE_DOWNLOADED_PATH = os.getenv('FILE_DOWNLOADED_PATH')
COST_PER_GB_TRANSFERRED = float(os.getenv('COST_PER_GB_TRANSFERRED'))

PATHQTY = 0
DATE_LIMIT = datetime.now(timezone.utc) - timedelta(days=DAYS_TO_FILTER)

# Load or initialize JSON files
def load_or_initialize_json(file_path, default_data):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    else:
        with open(file_path, 'w') as file:
            json.dump(default_data, file, indent=4)
        return default_data

data = load_or_initialize_json(FILE_SKIP_PATH, {"dateLimit": DATE_LIMIT.isoformat(), "skip": []})
validPath = load_or_initialize_json(FILE_VALID_PATH, {"path": []})
transfered_files = load_or_initialize_json(FILE_DOWNLOADED_PATH, {"transfered": []})
costs_data = load_or_initialize_json(COSTS_FILE_PATH, {"report": []})

# Update or initialize cost report
if not costs_data["report"]:
    new_report = {
        "baseDate": datetime.now(timezone.utc).isoformat(),
        "endMonth": (datetime.now(timezone.utc) + timedelta(days=31)).isoformat(),
        "startDate": datetime.now(timezone.utc).isoformat(),
        "endDate": "",
        "downloadedFiles": 0,
        "weightGB": "0.000",
        "remainingGB": "80.000",
        "estimatedCost": 0
    }
    costs_data["report"].append(new_report)
else:
    last_report = costs_data["report"][-1]
    if last_report["endMonth"] < datetime.now(timezone.utc).isoformat():
        new_report = copy.deepcopy(last_report)
        new_report.update({
            "startDate": datetime.now(timezone.utc).isoformat(),
            "baseDate": datetime.now(timezone.utc).isoformat(),
            "endMonth": (datetime.now(timezone.utc) + timedelta(days=31)).isoformat()
        })
        costs_data["report"].append(new_report)
data["dateLimit"] = DATE_LIMIT.isoformat()

# Initialize S3 client
s3 = boto3.client(
    's3', 
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Utility functions
def add_valid_path(path):
    if path not in validPath["path"]:
        validPath["path"].append(path)
        print(f"La ruta {path} ha sido agregada a las rutas válidas.")
    else:
        print(f"La ruta {path} ya se encuentra en las rutas válidas.")

def add_skip_path(prefix, last_modified):
    data["skip"].append({"path": prefix, "lastModified": last_modified.isoformat()})
    print(f"La ruta '{prefix}' ha sido agregada a las rutas inválidas.")

def remove_valid_path(path):
    if path in validPath["path"]:
        validPath["path"].remove(path)
        print(f"La ruta {path} ha sido eliminada de las rutas válidas.")

def remove_skip_path(prefix):
    for entry in data["skip"]:
        if entry["path"] == prefix:
            data["skip"].remove(entry)
            print(f"La ruta '{prefix}' ha sido borrada de las rutas inválidas.")
            break

def contains_prefix(data, prefix):
    path_set = {entry["path"] for entry in data["skip"]}
    return any(path.startswith(prefix) for path in path_set)

def manage_transfer():
    for path in validPath["path"]:
        list_files_in_s3(BUCKET_NAME, path)

def save_json(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

# Folder management functions
def get_last_modified_folder(response, prefix):
    if 'Contents' in response:
        last_modified_date = max(obj['LastModified'] for obj in response['Contents'])

        if contains_prefix(data, prefix):
            if last_modified_date < DATE_LIMIT:
                print(f"La ruta '{prefix}' es inválida, ha sido saltada.")
                return
            remove_skip_path(prefix)
            add_valid_path(prefix)
        else:
            if last_modified_date < DATE_LIMIT:
                add_skip_path(prefix, last_modified_date)
            else:
                add_valid_path(prefix)
    return False

def list_folders(bucket_name, prefix="", executor=None, futures=None):
    global PATHQTY

    if PATHQTY >= 10000:
        save_json(FILE_VALID_PATH, validPath)
        save_json(FILE_SKIP_PATH, data)
        PATHQTY = 0
    else:
        PATHQTY += 1

    continuation_token = None
    while True:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/',
            ContinuationToken=continuation_token
        ) if continuation_token else s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

        get_last_modified_folder(response, prefix)

        if 'CommonPrefixes' in response:
            for folder in response['CommonPrefixes']:
                if executor and futures is not None:
                    futures.append(executor.submit(list_folders, bucket_name, folder['Prefix'], executor, futures))
                else:
                    list_folders(bucket_name, folder['Prefix'])

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

def list_files_in_s3(bucket_name, prefix=""):
    global PATHQTY

    if PATHQTY > 20:
        save_json(FILE_VALID_PATH, validPath)
        save_json(FILE_SKIP_PATH, data)
        PATHQTY = 0

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    download_file_from_s3(bucket_name, obj['Key'])
                    concurrent.futures.wait(futures)
        drive = authenticate_google_drive()
        uploads_folder_id = get_or_create_folder(drive, 'uploads', 'root')
        upload_files_to_drive(drive, LOCAL_DOWNLOAD_PATH, uploads_folder_id)
        add_skip_path(prefix, DATE_LIMIT)
        remove_valid_path(prefix)
        delete_folder_contents(LOCAL_DOWNLOAD_PATH)
        save_json(COSTS_FILE_PATH, costs_data)
        save_json(FILE_DOWNLOADED_PATH, transfered_files)
        print(f"Descarga completa del directorio '{prefix}'.")
    else:
        print(f"No hay archivos en '{bucket_name}/{prefix}'.")

def download_file_from_s3(bucket_name, file_key, download_dir=LOCAL_DOWNLOAD_PATH):
    local_path = os.path.join(download_dir, file_key)
    file_name = file_key.split('/')[-1]

    if os.path.exists(local_path):
        print(f"El archivo '{file_name}' ya existe en '{local_path}'")
        return

    try:
        response = s3.head_object(Bucket=bucket_name, Key=file_key)
        last_modified = response['LastModified']
        file_size = response['ContentLength'] / (1024 ** 3)
    except Exception as e:
        print(f"Un error ha ocurrido: {e}")
        return

    if is_file_downloaded(file_key):
        print(f"El archivo '{file_name}' ya fue descargado anteriormente.")
        return

    if last_modified <= DATE_LIMIT:
        print(f"El archivo '{file_name}' no tiene una fecha válida.")
        return

    current_weight_gb = float(costs_data["report"][-1]["weightGB"])
    remaining_gb = float(costs_data["report"][-1]["remainingGB"])
    if file_size > remaining_gb:
        print(f"El archivo '{file_name}' pesa más del espacio restante para el mes.")
        return
    if current_weight_gb + file_size > 80.000:
        print(f"La descarga del archivo '{file_name}' supera el límite de 80 GB mensual.")
        return

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        s3.download_file(bucket_name, file_key, local_path)
        print(f"Archivo '{file_name}' descargado en: '{local_path}'")
        costs_data["report"][-1]["remainingGB"] = remaining_gb - file_size
        costs_data["report"][-1]["weightGB"] = current_weight_gb + file_size
        costs_data["report"][-1]["downloadedFiles"] += 1
        costs_data["report"][-1]["estimatedCost"] = costs_data["report"][-1]["weightGB"] * COST_PER_GB_TRANSFERRED

        transfered_files["transfered"].append({
            "path_to_file": file_key,
            "date_downloaded": datetime.now(timezone.utc).isoformat(),
            "date_uploaded": "",
            "status": "Downloaded",
            "error_message": ""
        })
    except Exception as e:
        transfered_files["transfered"].append({
            "path_to_file": file_key,
            "date_downloaded": datetime.now(timezone.utc).isoformat(),
            "date_uploaded": "",
            "status": "Download Error",
            "error_message": str(e)
        })
        print(f"Ocurrió un error durante la descarga: {e}")

def is_file_downloaded(path):
    transfered_set = {(entry["path_to_file"], entry["status"]) for entry in transfered_files.get("transfered", [])}
    return (path, "Downloaded") in transfered_set or (path, "Uploaded") in transfered_set

def delete_folder_contents(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path, onerror=on_rm_error)
        os.makedirs(folder_path)

def on_rm_error(func, path, exc_info):
    os.chmod(path, stat.S_IWRITE)
    func(path)

# Google Drive Functions
def authenticate_google_drive():
    gauth = GoogleAuth()
    if os.path.exists("credentials.json"):
        gauth.LoadCredentialsFile("credentials.json")
        if gauth.credentials is None or gauth.access_token_expired:
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile("credentials.json")
        else:
            gauth.Authorize()
    else:
        gauth.LoadClientConfigFile("client_secrets.json")
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile("credentials.json")
    return GoogleDrive(gauth)

def get_or_create_folder(drive, folder_name, parent_folder_id):
    query = f"'{parent_folder_id}' in parents and title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    if file_list:
        return file_list[0]['id']
    else:
        return create_folder(drive, folder_name, parent_folder_id)

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

def upload_files_to_drive(drive, local_path, parent_folder_id=None):
    for root, dirs, files in os.walk(local_path):
        path_from_local = os.path.relpath(root, local_path)
        folders = path_from_local.split(os.sep)
        current_folder_id = parent_folder_id
        for folder in folders:
            if folder != '.':
                current_folder_id = get_or_create_folder(drive, folder, current_folder_id)
        for file in files:
            file_path = os.path.join(root, file)
            file_drive = drive.CreateFile({'title': file, 'parents': [{'id': current_folder_id}]})
            file_drive.SetContentFile(file_path)
            try:
                file_drive.Upload()
                print(f'Archivo "{file_path}" subido a Google Drive.')
                relative_file_path = os.path.relpath(file_path, LOCAL_DOWNLOAD_PATH).replace("\\", "/")
                for entry in transfered_files["transfered"]:
                    if entry["path_to_file"] == relative_file_path:
                        entry.update({
                            "status": "Uploaded",
                            "date_uploaded": datetime.now(timezone.utc).isoformat(),
                            "error_message": ""
                        })
                        break
            except Exception as e:
                print(f'Ocurrió un error durante la subida "{file_path}": {e}')
                relative_file_path = os.path.relpath(file_path, LOCAL_DOWNLOAD_PATH).replace("\\", "/")
                for entry in transfered_files["transfered"]:
                    if entry["path_to_file"] == relative_file_path:
                        entry.update({
                            "status": "Upload Error",
                            "error_message": str(e)
                        })
                        break

# Main execution
if __name__ == '__main__':
    # Uncomment the following block to enable concurrent execution
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     futures = []
    #     list_folders(BUCKET_NAME, PREFIX, executor, futures)
    #     concurrent.futures.wait(futures)

    manage_transfer()
    save_json(FILE_VALID_PATH, validPath)
    save_json(FILE_SKIP_PATH, data)
    save_json(COSTS_FILE_PATH, costs_data)
    save_json(FILE_DOWNLOADED_PATH, transfered_files)
    print("Script terminado.")
