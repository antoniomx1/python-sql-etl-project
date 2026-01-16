import logging
import os
from dotenv import load_dotenv
from extract import extract_excel_sheet, extract_json_data
from transform import transform_data
from load import load_data_to_postgres

# Carga variables de entorno (para local y para que lea los secretos)
load_dotenv()

# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Orquestador principal. 
    Detecta entorno (Local vs Nube) y gestiona la ingesta desde Drive si es necesario.
    """
    logger.info("--- INICIANDO PIPELINE DE DATOS (HYBRID CLOUD) ---")

    # Rutas locales esperadas
    path_excel = "data/ClientesMarca.xlsx"
    path_json = "data/RecomendadosMarca.json"

    # ID de la carpeta en Google Drive (Vital para GitHub Actions)
    # Si esto es None, el script solo funcionará si los archivos ya existen localmente.
    drive_folder_id = os.getenv("DRIVE_FOLDER_ID")

    if not drive_folder_id:
        logger.warning("No se detectó DRIVE_FOLDER_ID. El script dependerá de archivos locales.")

    logger.info("Fase 1: Ingesta de datos...")
    
    # Pasamos el ID de la carpeta. El extract.py decidirá si descarga o lee local.
    df_clientes = extract_excel_sheet(path_excel, "Clientes", drive_folder_id)
    df_transacciones = extract_excel_sheet(path_excel, "Transacciones", drive_folder_id)
    df_varios = extract_excel_sheet(path_excel, "Varios", drive_folder_id)
    df_recomendados = extract_json_data(path_json, drive_folder_id)

    # Validación de integridad
    if any(df is None for df in [df_clientes, df_transacciones, df_varios, df_recomendados]):
        logger.error("Falla crítica: No se pudieron obtener todas las fuentes de datos. Abortando.")
        return

    logger.info("Fase 2: Transformación y Lógica de Negocio...")
    data_processed = transform_data(
        df_clientes, 
        df_transacciones, 
        df_varios, 
        df_recomendados
    )

    if data_processed:
        logger.info("Fase 3: Carga a Data Warehouse (Supabase)...")
        load_data_to_postgres(data_processed)
        logger.info("--- PIPELINE FINALIZADO CON ÉXITO ---")
    else:
        logger.error("Error en la transformación de datos.")

if __name__ == "__main__":
    run_pipeline()