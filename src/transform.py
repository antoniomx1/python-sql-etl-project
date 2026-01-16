import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

def transform_data(
    df_clientes: pd.DataFrame, 
    df_transacciones: pd.DataFrame, 
    df_varios: pd.DataFrame, 
    df_recomendados: pd.DataFrame
) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Transformación con Red de Seguridad para IDs huérfanos.
    """
    try:
        logger.info("Iniciando orquestación de transformaciones...")

        # --- 1. Segmentación de Catálogos (Archivo Varios) ---
        split_search = df_varios[df_varios.iloc[:, 0] == 'ID'].index
        
        if not split_search.empty:
            cut_point = split_search[0]
            # Parte 1: SEDES
            df_sedes = df_varios.iloc[:cut_point].copy()
            df_sedes.columns = ['id_sede', 'nombre_sede']
            
            # Parte 2: TIPOS DE TRANSACCION
            df_tipos = df_varios.iloc[cut_point+1:].copy()
            df_tipos.columns = ['id_tipo_trx', 'descripcion_tipo']
        else:
            # Fallback por si el archivo viene raro
            df_sedes = pd.DataFrame(columns=['id_sede', 'nombre_sede'])
            df_tipos = pd.DataFrame(columns=['id_tipo_trx', 'descripcion_tipo'])

        # --- RED DE SEGURIDAD (AQUÍ ARREGLAMOS EL ERROR DEL ID 23) ---
        # 1. Identificamos qué IDs vienen en las transacciones
        trx_ids = df_transacciones.iloc[:, 2].unique() # Asumimos col 2 es id_tipo_trx por posición en Excel
        
        # 2. Identificamos qué IDs tenemos en el catálogo
        # Limpiamos y convertimos a int para poder comparar
        df_tipos = df_tipos.dropna(subset=['id_tipo_trx'])
        df_tipos['id_tipo_trx'] = df_tipos['id_tipo_trx'].astype(int)
        catalog_ids = set(df_tipos['id_tipo_trx'])
        
        # 3. Encontramos los huérfanos (están en trx pero no en catálogo)
        missing_ids = [x for x in trx_ids if x not in catalog_ids and pd.notna(x)]
        
        if missing_ids:
            logger.warning(f"¡OJO! Se encontraron IDs huérfanos: {missing_ids}. Creando registros dummy.")
            # Creamos un DataFrame con los faltantes
            df_missing = pd.DataFrame({
                'id_tipo_trx': missing_ids,
                'descripcion_tipo': ['Tipo Desconocido (Autogenerado)'] * len(missing_ids)
            })
            # Los pegamos al catálogo oficial
            df_tipos = pd.concat([df_tipos, df_missing], ignore_index=True)

        # --- 2. Normalización de Distribuidores ---
        df_dist = df_recomendados[
            ['IDDISTRIBUIDOR', 'NOMBRE DISTRIBUIDOR', 'TELEFONO', 'categoría']
        ].drop_duplicates(subset=['IDDISTRIBUIDOR'])
        df_dist.columns = ['id_distribuidor', 'nombre_distribuidor', 'telefono', 'categoria']

        # --- 3. Enriquecimiento de Clientes ---
        df_clientes.columns = ['id_cliente', 'fecha_afiliacion', 'fecha_primera_trx']
        df_clientes_final = pd.merge(
            df_clientes, 
            df_recomendados[['IDCLIENTE', 'IDDISTRIBUIDOR']], 
            left_on='id_cliente', 
            right_on='IDCLIENTE', 
            how='left'
        ).drop(columns=['IDCLIENTE'])
        df_clientes_final.columns = ['id_cliente', 'fecha_afiliacion', 'fecha_primera_trx', 'id_distribuidor']

        # --- 4. Tabla de Hechos ---
        df_transacciones.columns = [
            'id_cliente', 'fecha_trx', 'id_tipo_trx', 'id_trx', 'monto', 'fee', 'id_sede'
        ]

        # --- 5. Casting Final ---
        for col in ['fecha_afiliacion', 'fecha_primera_trx']:
            df_clientes_final[col] = pd.to_datetime(df_clientes_final[col]).dt.date
        df_transacciones['fecha_trx'] = pd.to_datetime(df_transacciones['fecha_trx'])
        
        df_sedes = df_sedes.dropna(subset=['id_sede'])
        df_sedes['id_sede'] = df_sedes['id_sede'].astype(int)
        
        # Aseguramos que la tabla de hechos tenga los tipos correctos
        df_transacciones['id_tipo_trx'] = df_transacciones['id_tipo_trx'].astype(int)

        logger.info("Transformación finalizada exitosamente.")
        
        # OJO AQUÍ: Cambiamos el nombre de la llave para que coincida con tu DBeaver
        return {
            "dim_sedes": df_sedes,
            "dim_tipo_transaccion": df_tipos,  
            "dim_distribuidores": df_dist,
            "dim_clientes": df_clientes_final,
            "fct_transacciones": df_transacciones
        }

    except Exception as e:
        logger.error(f"Falla crítica en transformación: {str(e)}")
        return None