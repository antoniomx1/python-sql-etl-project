import pandas as pd
import logging
from typing import Dict, Optional

# Configuracion de logger a nivel modulo
logger = logging.getLogger(__name__)

def transform_data(
    df_clientes: pd.DataFrame, 
    df_transacciones: pd.DataFrame, 
    df_varios: pd.DataFrame, 
    df_recomendados: pd.DataFrame
) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Ejecuta la limpieza y transformacion de datos aplicando reglas de negocio.
    """
    try:
        logger.info("Iniciando proceso de transformacion de datos.")

        # --- 1. Procesamiento de Archivo Mixto (Varios) ---
        # Buscamos DONDE estan los encabezados 'ID'
        # Probablemente encuentre indices como [0, 5] (ejemplo)
        split_search = df_varios[df_varios.iloc[:, 0] == 'ID'].index
        
        if len(split_search) > 1:
            # Caso Normal: Encontro dos 'ID' (el del inicio y el de en medio)
            # El corte real es el SEGUNDO 'ID' (split_search[1])
            cut_point = split_search[1]
            
            # Tabla 1 (Sedes): Desde fila 1 (saltando el primer header) hasta el corte
            df_sedes = df_varios.iloc[1:cut_point].copy()
            df_sedes.columns = ['id_sede', 'nombre_sede']
            
            # Tabla 2 (Tipos): Desde corte+1 (saltando el segundo header) hasta el final
            df_tipos = df_varios.iloc[cut_point+1:].copy()
            df_tipos.columns = ['id_tipo_trx', 'descripcion_tipo']
            
        elif len(split_search) == 1:
            # Caso Raro: Solo hay un header. Asumimos que corto al inicio y todo es una sola tabla?
            # Por seguridad, si pasa esto, mejor partimos asumiendo estructura conocida o fallamos controlado.
            logger.warning("Solo se encontro un encabezado 'ID'. Intentando logica fallback.")
            cut_point = split_search[0]
            if cut_point == 0:
                 # Si el ID esta en la 0, asumimos que NO hay segunda tabla, solo Sedes?
                 # Esto es un parche, pero evita el crash.
                 df_sedes = df_varios.iloc[1:].copy()
                 df_sedes.columns = ['id_sede', 'nombre_sede']
                 df_tipos = pd.DataFrame(columns=['id_tipo_trx', 'descripcion_tipo'])
            else:
                 df_sedes = df_varios.iloc[:cut_point].copy()
                 df_tipos = df_varios.iloc[cut_point+1:].copy()
        else:
            logger.warning("Formato de archivo Varios no estandar (Sin headers 'ID'). Se generan dataframes vacios.")
            df_sedes = pd.DataFrame(columns=['id_sede', 'nombre_sede'])
            df_tipos = pd.DataFrame(columns=['id_tipo_trx', 'descripcion_tipo'])

        # --- 2. Validacion de Integridad (IDs Huérfanos) ---
        trx_ids = df_transacciones.iloc[:, 2].unique() 
        
        # Limpieza y casting SEGURO (Manejo de errores si viene basura)
        df_tipos = df_tipos.dropna(subset=['id_tipo_trx'])
        # Convertimos a string primero para quitar basura y luego a int
        df_tipos = df_tipos[pd.to_numeric(df_tipos['id_tipo_trx'], errors='coerce').notnull()]
        df_tipos['id_tipo_trx'] = df_tipos['id_tipo_trx'].astype(int)
        
        catalog_ids = set(df_tipos['id_tipo_trx'])
        
        missing_ids = [x for x in trx_ids if x not in catalog_ids and pd.notna(x)]
        
        if missing_ids:
            logger.warning(f"Integridad Referencial: IDs huérfanos detectados: {missing_ids}. Generando dummies.")
            df_missing = pd.DataFrame({
                'id_tipo_trx': missing_ids,
                'descripcion_tipo': ['Tipo Desconocido (Sistema)'] * len(missing_ids)
            })
            df_tipos = pd.concat([df_tipos, df_missing], ignore_index=True)

        # --- 3. Normalizacion de Dimension Distribuidores ---
        df_dist = df_recomendados[['IDDISTRIBUIDOR', 'NOMBRE DISTRIBUIDOR']].drop_duplicates(subset=['IDDISTRIBUIDOR'])
        df_dist.columns = ['id_distribuidor', 'nombre_distribuidor']

        # --- 4. Construccion de Dimension Clientes ---
        df_clientes_base = df_clientes.rename(columns={
            'IDCLIENTE': 'id_cliente', 
            'fechaafiliacion': 'fecha_afiliacion', 
            'fechaprimertrx': 'fecha_primera_trx'
        })

        df_json_subset = df_recomendados[['IDCLIENTE', 'IDDISTRIBUIDOR', 'TELEFONO', 'categoría', 'recomendados']]
        
        df_clientes_final = pd.merge(
            df_clientes_base, 
            df_json_subset, 
            left_on='id_cliente', 
            right_on='IDCLIENTE', 
            how='left'
        )
        
        if 'IDCLIENTE' in df_clientes_final.columns:
            df_clientes_final = df_clientes_final.drop(columns=['IDCLIENTE'])
            
        df_clientes_final = df_clientes_final.rename(columns={
            'IDDISTRIBUIDOR': 'id_distribuidor',
            'TELEFONO': 'telefono',
            'categoría': 'categoria'
        })

        # --- 5. Estandarizacion de Tabla de Hechos ---
        df_transacciones.columns = [
            'id_cliente', 'fecha_trx', 'id_tipo_trx', 'id_trx', 'monto', 'fee', 'id_sede'
        ]

        # --- 6. Casting de Tipos de Datos ---
        for col in ['fecha_afiliacion', 'fecha_primera_trx']:
            df_clientes_final[col] = pd.to_datetime(df_clientes_final[col], errors='coerce').dt.date
        
        df_transacciones['fecha_trx'] = pd.to_datetime(df_transacciones['fecha_trx'], errors='coerce')
        
        # Casting seguro para Sedes tambien
        df_sedes = df_sedes.dropna(subset=['id_sede'])
        df_sedes = df_sedes[pd.to_numeric(df_sedes['id_sede'], errors='coerce').notnull()]
        df_sedes['id_sede'] = df_sedes['id_sede'].astype(int)
        
        df_transacciones['id_tipo_trx'] = df_transacciones['id_tipo_trx'].astype(int)

        logger.info("Transformacion de datos completada.")
        
        return {
            "dim_sedes": df_sedes,
            "dim_tipo_transaccion": df_tipos,  
            "dim_distribuidores": df_dist,
            "dim_clientes": df_clientes_final,
            "fct_transacciones": df_transacciones
        }

    except Exception as e:
        logger.error(f"Error critico durante la transformacion: {str(e)}")
        # Importante: Levantar la excepcion para que el main se entere y no siga
        raise e