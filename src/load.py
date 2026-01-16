import pandas as pd
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

def get_existing_ids(table_name, pk_col, engine):
    """
    Consulta la base de datos para ver qué IDs ya existen.
    Retorna un set de IDs para búsqueda rápida.
    """
    try:
        query = f"SELECT {pk_col} FROM {table_name}"
        existing_df = pd.read_sql(query, engine)
        return set(existing_df[pk_col].tolist())
    except Exception:
        # Si la tabla no existe o está vacía, retornamos set vacío
        return set()

def load_to_sql(df, table_name, engine):
    """
    Carga INCREMENTAL.
    1. Revisa qué registros ya existen (usando la PK).
    2. Filtra los duplicados.
    3. Solo inserta lo nuevo (Append).
    """
    try:
        # 1. Definir cuál es la Primary Key de cada tabla para validar
        # Mapeo manual de tus tablas vs sus PKs
        pk_map = {
            'dim_sedes': 'id_sede',
            'dim_tipo_transaccion': 'id_tipo_trx',
            'dim_distribuidores': 'id_distribuidor',
            'dim_clientes': 'id_cliente',
            'fct_transacciones': 'id_trx'
        }

        if table_name not in pk_map:
            logger.warning(f"Tabla {table_name} no tiene PK definida en el script. Se intenta append directo.")
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True

        pk_col = pk_map[table_name]
        
        # 2. Obtener IDs existentes
        logger.info(f"Validando duplicados en {table_name}...")
        existing_ids = get_existing_ids(table_name, pk_col, engine)
        
        # 3. Filtrar: Quedarse solo con lo que NO existe en la base
        # (El símbolo ~ significa negación: "Donde la columna NO esté en existing_ids")
        df_new = df[~df[pk_col].isin(existing_ids)]
        
        count_new = len(df_new)
        count_ignored = len(df) - count_new

        if count_new > 0:
            logger.info(f"Insertando {count_new} registros nuevos en {table_name} (Ignorados: {count_ignored}).")
            df_new.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        else:
            logger.info(f"Tabla {table_name} al día. No hay registros nuevos que insertar.")
            return True
            
    except Exception as e:
        logger.error(f"Error en carga incremental de {table_name}: {str(e)}")
        # Importante: No retornamos False para no matar el pipeline completo si una tabla falla, 
        # pero loggeamos el error. O si prefieres estricto, retorna False.
        return False

def create_db_engine():
    # Tu funcion de siempre para crear el engine
    from sqlalchemy import create_engine
    import os
    
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)