import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Importaciones locales
import extract
import transform
import load

# Configuracion global de logging
if not os.path.exists('logs'):
    os.makedirs('logs')

log_filename = f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Orquestador principal del proceso ETL.
    """
    try:
        logger.info("--- INICIO DEL PIPELINE DE DATOS ---")
        
        # 1. EXTRACT
        logger.info("Fase 1: Extraccion de datos")
        load_dotenv()
        
        # Validacion de credenciales criticas
        if not os.getenv('DB_HOST'):
            raise ValueError("No se encontraron variables de entorno para la base de datos.")

        # Descarga/Lectura de fuentes
        # Nota: extract.extract_data debe retornar los 4 dataframes esperados
        df_clientes, df_transacciones, df_varios, df_recomendados = extract.extract_data()
        
        if df_clientes is None:
            raise ValueError("Fallo critico en la extraccion de archivos.")

        # 2. TRANSFORM
        logger.info("Fase 2: Transformacion y Reglas de Negocio")
        # Recibimos el diccionario con las tablas listas
        data_warehouse_tables = transform.transform_data(
            df_clientes, 
            df_transacciones, 
            df_varios, 
            df_recomendados
        )
        
        if not data_warehouse_tables:
            raise ValueError("La transformacion retorno datos vacios o nulos.")

        # 3. LOAD
        logger.info("Fase 3: Carga a Base de Datos (Full Refresh)")
        engine = load.create_db_engine()
        
        if not engine:
            raise ConnectionError("No se pudo establecer conexion a la base de datos.")

        # Orden estricto de carga para respetar integridad referencial (FKs)
        # 1. Catalogos Independientes (Dimensiones Padreas)
        ordered_load = [
            ('dim_sedes', data_warehouse_tables['dim_sedes']),
            ('dim_tipo_transaccion', data_warehouse_tables['dim_tipo_transaccion']),
            ('dim_distribuidores', data_warehouse_tables['dim_distribuidores']),
            # 2. Entidades Dependientes (Dimensiones Hijas)
            ('dim_clientes', data_warehouse_tables['dim_clientes']),
            # 3. Hechos (Tabla Central)
            ('fct_transacciones', data_warehouse_tables['fct_transacciones'])
        ]

        success_count = 0
        for table_name, df in ordered_load:
            if not df.empty:
                result = load.load_to_sql(df, table_name, engine)
                if result:
                    success_count += 1
            else:
                logger.warning(f"La tabla {table_name} esta vacia. Se omite carga.")

        if success_count == len(ordered_load):
            logger.info(f"--- PIPELINE FINALIZADO CON EXITO ({success_count}/{len(ordered_load)} tablas) ---")
        else:
            logger.warning(f"Pipeline finalizado con advertencias. Tablas cargadas: {success_count}/{len(ordered_load)}")

    except Exception as e:
        logger.critical(f"El Pipeline fallo: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()