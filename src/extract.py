import pandas as pd
import json
import logging
import os
from typing import Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv

# Carga de variables de entorno explícita para asegurar disponibilidad
load_dotenv()

logger = logging.getLogger(__name__)

# Definición de alcances requeridos por la API de Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']

def _get_drive_service():
    """
    Construye y retorna el servicio de la API de Google Drive.
    Implementa una estrategia de autenticación híbrida:
    1. Intenta cargar credenciales desde variable de entorno (Entornos CI/CD).
    2. Intenta cargar desde archivo local (Entorno de desarrollo).
    """
    try:
        # Estrategia 1: Variable de Entorno (Producción / GitHub Actions)
        # Se espera que el contenido del JSON esté en la variable 'GCP_SA_KEY'
        env_credentials = os.getenv("GCP_SA_KEY")
        if env_credentials:
            logger.info("Autenticando vía Variable de Entorno (Cloud/CI).")
            info = json.loads(env_credentials)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)

        # Estrategia 2: Archivo Local (Desarrollo)
        local_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "google_credentials.json")
        if os.path.exists(local_file):
            logger.info(f"Autenticando vía Archivo Local: {local_file}")
            creds = service_account.Credentials.from_service_account_file(local_file, scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)

        logger.error("No se encontraron credenciales válidas (ni archivo local ni variable de entorno).")
        return None

    except Exception as e:
        logger.error(f"Error al inicializar servicio de Google Drive: {str(e)}")
        return None

def download_file_from_drive(file_name: str, folder_id: str, local_path: str) -> bool:
    """
    Busca un archivo específico en una carpeta de Google Drive y lo descarga localmente.
    
    Args:
        file_name: Nombre exacto del archivo a buscar.
        folder_id: ID de la carpeta de Drive contenedora.
        local_path: Ruta local donde se guardará el archivo descargado.
        
    Returns:
        bool: True si la descarga fue exitosa, False en caso contrario.
    """
    service = _get_drive_service()
    if not service:
        return False

    try:
        # Consulta filtrada para evitar descargas incorrectas o de la papelera
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            logger.warning(f"Archivo '{file_name}' no encontrado en la carpeta remota especificada.")
            return False

        # Se selecciona la primera coincidencia
        file_id = items[0]['id']
        logger.info(f"Iniciando descarga de '{file_name}' (Drive ID: {file_id})...")
        
        request = service.files().get_media(fileId=file_id)
        
        # Escritura binaria del archivo
        with open(local_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        
        logger.info(f"Archivo descargado exitosamente en: {local_path}")
        return True

    except Exception as e:
        logger.error(f"Excepción durante la descarga desde Drive: {str(e)}")
        return False

def extract_excel_sheet(file_path: str, sheet_name: str, drive_folder_id: str = None) -> Optional[pd.DataFrame]:
    """
    Orquesta la extracción de datos desde un archivo Excel.
    Si el archivo no existe localmente y se provee un ID de Drive, intenta descargarlo primero.
    """
    # Verificación de existencia local y recuperación fallback desde la nube
    if not os.path.exists(file_path) and drive_folder_id:
        file_name = os.path.basename(file_path)
        logger.info(f"Archivo local no encontrado. Intentando recuperar '{file_name}' desde Drive.")
        success = download_file_from_drive(file_name, drive_folder_id, file_path)
        if not success:
            logger.error("No fue posible obtener el archivo fuente.")
            return None

    try:
        if not os.path.exists(file_path):
            logger.error(f"El archivo fuente no está disponible: {file_path}")
            return None
            
        # Lectura optimizada usando el motor openpyxl
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        logger.info(f"Extracción exitosa: {len(df)} registros obtenidos de '{sheet_name}'")
        return df
    except Exception as e:
        logger.error(f"Error de lectura en archivo Excel ({sheet_name}): {str(e)}")
        return None

def extract_json_data(file_path: str, drive_folder_id: str = None) -> Optional[pd.DataFrame]:
    """
    Orquesta la extracción de datos desde un archivo JSON.
    Incluye lógica de recuperación desde la nube si el archivo local está ausente.
    """
    if not os.path.exists(file_path) and drive_folder_id:
        file_name = os.path.basename(file_path)
        download_file_from_drive(file_name, drive_folder_id, file_path)

    try:
        if not os.path.exists(file_path):
            logger.error(f"El archivo JSON fuente no está disponible: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        logger.info(f"Extracción exitosa: {len(df)} registros obtenidos desde JSON")
        return df
    except Exception as e:
        logger.error(f"Error de parseo en archivo JSON: {str(e)}")
        return None