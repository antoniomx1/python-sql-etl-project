import pandas as pd
import json
import logging
import os
from typing import Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv

# Carga explicita de variables de entorno
load_dotenv()

logger = logging.getLogger(__name__)

# Definicion de alcances requeridos por la API de Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']

def _get_drive_service():
    """
    Construye y retorna el servicio de la API de Google Drive.
    """
    try:
        # Estrategia 1: Variable de Entorno (Produccion / CI)
        env_credentials = os.getenv("GCP_SA_KEY")
        if env_credentials:
            logger.info("Autenticando via Variable de Entorno (GCP_SA_KEY).")
            info = json.loads(env_credentials)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)

        # Estrategia 2: Archivo Local (Desarrollo)
        local_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "google_credentials.json")
        if os.path.exists(local_file):
            logger.info(f"Autenticando via Archivo Local: {local_file}")
            creds = service_account.Credentials.from_service_account_file(local_file, scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)

        logger.warning("No se encontraron credenciales de Drive. Se intentara modo local estricto.")
        return None

    except Exception as e:
        logger.error(f"Error al inicializar servicio de Google Drive: {str(e)}")
        return None

def download_file_from_drive(file_name: str, folder_id: str, local_path: str) -> bool:
    """
    Descarga un archivo especifico desde Google Drive.
    """
    service = _get_drive_service()
    if not service:
        return False

    try:
        # Consulta para encontrar el archivo dentro de la carpeta especifica
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            logger.warning(f"Archivo '{file_name}' no encontrado en carpeta Drive ID: {folder_id}")
            return False

        file_id = items[0]['id']
        logger.info(f"Descargando '{file_name}' (ID: {file_id})...")
        
        request = service.files().get_media(fileId=file_id)
        
        # Asegurar que el directorio destino exista
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        
        logger.info(f"Descarga completada: {local_path}")
        return True

    except Exception as e:
        logger.error(f"Error en descarga de Drive: {str(e)}")
        return False

def extract_excel_sheet(file_path: str, sheet_name: str, drive_folder_id: str = None, **kwargs) -> Optional[pd.DataFrame]:
    """
    Extrae una hoja de Excel. Si no existe localmente, intenta descargarla.
    Acepta kwargs para pasar parametros a pd.read_excel (ej. header=None).
    """
    # Intentar descarga si no existe localmente
    if not os.path.exists(file_path) and drive_folder_id:
        file_name = os.path.basename(file_path)
        download_file_from_drive(file_name, drive_folder_id, file_path)

    try:
        if not os.path.exists(file_path):
            logger.error(f"Archivo no disponible localmente: {file_path}")
            return None
            
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl', **kwargs)
        logger.info(f"Hoja '{sheet_name}' leida correctamente: {len(df)} filas.")
        return df
    except Exception as e:
        logger.error(f"Error leyendo Excel hoja '{sheet_name}': {str(e)}")
        return None

def extract_json_data(file_path: str, drive_folder_id: str = None) -> Optional[pd.DataFrame]:
    """
    Extrae datos de un JSON. Si no existe localmente, intenta descargarlo.
    """
    if not os.path.exists(file_path) and drive_folder_id:
        file_name = os.path.basename(file_path)
        download_file_from_drive(file_name, drive_folder_id, file_path)

    try:
        if not os.path.exists(file_path):
            logger.error(f"Archivo JSON no disponible: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        logger.info(f"JSON leido correctamente: {len(df)} registros.")
        return df
    except Exception as e:
        logger.error(f"Error leyendo JSON: {str(e)}")
        return None

def extract_data() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Funcion Orquestadora Principal.
    Encargada de obtener todos los dataframes necesarios para el pipeline.
    """
    logger.info("Iniciando fase de extraccion de datos...")
    
    # Configuracion de rutas
    folder_id = os.getenv("DRIVE_FOLDER_ID")
    excel_path = 'data/ClientesMarca.xlsx'
    json_path = 'data/RecomendadosMarca.json'

    # 1. Clientes
    df_clientes = extract_excel_sheet(excel_path, 'Clientes', folder_id)
    
    # 2. Transacciones
    df_transacciones = extract_excel_sheet(excel_path, 'Transacciones', folder_id)
    
    # 3. Varios (Sedes y Tipos) - IMPORTANTE: header=None porque es estructura mixta
    df_varios = extract_excel_sheet(excel_path, 'Varios', folder_id, header=None)
    
    # 4. JSON Recomendados
    df_recomendados = extract_json_data(json_path, folder_id)
    
    # Verificacion final
    if any(df is None for df in [df_clientes, df_transacciones, df_varios, df_recomendados]):
        logger.critical("Fallo la extraccion de uno o mas archivos fuente.")
        return None, None, None, None
        
    return df_clientes, df_transacciones, df_varios, df_recomendados