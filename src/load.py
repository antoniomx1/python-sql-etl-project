import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
from typing import Dict, Optional
import pandas as pd

load_dotenv()
logger = logging.getLogger(__name__)

def load_data_to_postgres(data_dict: Optional[Dict[str, pd.DataFrame]]):
    """
    Carga los datos a Supabase.
    Sin saltos, sin rodeos. Si algo falta, avisa, pero aquí alineamos los nombres.
    """
    if data_dict is None:
        logger.error("Error: No llegaron datos de la transformación.")
        return

    try:
        # 1. Conexión
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        connection_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(connection_url)
        
        logger.info("Conectando a Supabase...")

        # 2. Lista de tablas EXACTA como están en tu Base de Datos
        # OJO: Aquí estaba el error, ya está corregido.
        tables_to_load = [
            "dim_sedes", 
            "dim_tipo_transaccion",  # <--- AHORA SÍ COINCIDE CON TU SQL
            "dim_distribuidores", 
            "dim_clientes"
        ]
        
        # 3. Carga de Dimensiones
        for table_name in tables_to_load:
            if table_name in data_dict:
                logger.info(f"Subiendo tabla: {table_name}...")
                data_dict[table_name].to_sql(
                    table_name, 
                    engine, 
                    if_exists='append', 
                    index=False, 
                    method='multi'
                )
                logger.info(f"-> {table_name}: LISTO")
            else:
                # Si esto sale, es porque la transformación no mandó la tabla
                logger.error(f"¡ALERTA! La tabla {table_name} no se encontró en los datos procesados.")

        # 4. Carga de Hechos (Al final por las llaves foráneas)
        if "fct_transacciones" in data_dict:
            logger.info("Subiendo tabla de hechos: fct_transacciones...")
            data_dict["fct_transacciones"].to_sql(
                "fct_transacciones", 
                engine, 
                if_exists='append', 
                index=False, 
                method='multi'
            )
            logger.info("-> fct_transacciones: LISTO")

        logger.info("--- CARGA COMPLETA SIN ERRORES ---")

    except Exception as e:
        logger.error(f"Error crítico en la BD: {str(e)}")