import pandas as pd
import numpy as np
import time
import math
import logging
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import HTTPException
import psycopg2
from psycopg2.extras import execute_batch

from database import get_db_connection
from config import DATABASE_CONFIG as DB_CONFIG

logger = logging.getLogger(__name__)

def get_mode_description_for_report(mode: str) -> str:
    """Obtener descripción legible del modo de procesamiento para el informe."""
    if mode == 'insert':
        return "Subir en limpio (Truncar tabla antes de insertar)"
    elif mode == 'update':
        return "Actualizar registros existentes"
    elif mode == 'sync':
        return "Sincronizar (Insertar nuevos y actualizar existentes)"
    return mode

def clean_table_for_insert(table_name: str, conn) -> None:
    """Limpiar tabla para inserción en modo 'insert' (subir en limpio)"""
    cursor = conn.cursor()
    try:
        conn.rollback()
        conn.autocommit = True
        cursor.execute(f'ALTER TABLE "{table_name}" DISABLE TRIGGER ALL')
        conn.autocommit = False
        cursor.execute("BEGIN")
        cursor.execute("SET CONSTRAINTS ALL DEFERRED")
        cursor.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
        conn.commit()
        conn.autocommit = True
        cursor.execute(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL')
        conn.autocommit = False
        logger.info(f"Tabla {table_name} truncada exitosamente")
    except Exception as e:
        conn.autocommit = False
        conn.rollback()
        try:
            conn.autocommit = True
            cursor.execute(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL')
            conn.autocommit = False
        except:
            pass
        logger.error(f"Error truncando tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error limpiando tabla: {str(e)}")
    finally:
        conn.autocommit = False

def resolve_foreign_keys_batch(df_batch: pd.DataFrame, fk_mappings: Dict, conn) -> pd.DataFrame:
    """Resolver foreign keys para un lote de datos de manera optimizada"""
    if not fk_mappings:
        return df_batch
    
    df_resolved = df_batch.copy()
    cursor = conn.cursor()
    
    try:
        for col, fk_info in fk_mappings.items():
            if col not in df_resolved.columns:
                continue
                
            foreign_table = fk_info['foreign_table']
            foreign_column = fk_info['foreign_column']
            lookup_column = fk_info['lookup_column']
            
            unique_values = df_resolved[col].dropna().unique()
            if len(unique_values) == 0:
                continue
            
            def clean_value_for_query(val):
                if isinstance(val, (np.integer, np.int64)):
                    return int(val)
                elif isinstance(val, (np.floating, np.float64, float)):
                    if val == int(val):
                        return int(val)
                    else:
                        return int(round(val))
                elif isinstance(val, str):
                    try:
                        float_val = float(val)
                        if float_val == int(float_val):
                            return int(float_val)
                        else:
                            return int(round(float_val))
                    except (ValueError, TypeError):
                        return val
                else:
                    return val
            
            native_values = [clean_value_for_query(val) for val in unique_values]
            
            placeholders = ','.join(['%s'] * len(native_values))
            lookup_query = f'''
                SELECT "{lookup_column}", "{foreign_column}" 
                FROM "{foreign_table}" 
                WHERE "{lookup_column}" IN ({placeholders})
            '''
            
            cursor.execute(lookup_query, native_values)
            lookup_results = dict(cursor.fetchall())
            
            df_resolved[col] = df_resolved[col].map(lookup_results)
            logger.info(f"[FK_BATCH] Resueltos {len(lookup_results)} valores para columna {col}")
    
    except Exception as e:
        logger.error(f"[FK_BATCH] Error resolviendo FKs: {e}")
        raise
    finally:
        cursor.close()
    
    return df_resolved

def process_batch_insert(batch_data: List[Dict], table_name: str, columns: List[str], 
                        conn_params: Dict, batch_id: int) -> Dict:
    """Procesar un lote de datos para inserción optimizada con manejo de errores a nivel de fila."""
    start_time = time.time()
    conn = None
    cursor = None
    
    row_results = []
    rows_inserted = 0
    rows_with_errors = 0

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        columns_str = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'

        for i, row_data in enumerate(batch_data):
            row_number = row_data.get('_row_number_excel', i + 1)
            try:
                values = [row_data.get(col) for col in columns]
                cleaned_values = [None if pd.isna(v) else (int(v) if isinstance(v, float) and v == int(v) else v) for v in values]

                cursor.execute(insert_query, cleaned_values)
                rows_inserted += 1
                row_results.append({
                    "row_number_excel": row_number,
                    "status": "inserted",
                    "message": "",
                    "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}
                })
            except Exception as row_error:
                conn.rollback()
                rows_with_errors += 1
                row_results.append({
                    "row_number_excel": row_number,
                    "status": "error",
                    "message": f"Error en fila: {str(row_error).strip()}",
                    "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}
                })
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"[BATCH_{batch_id}] Error catastrófico en lote: {e}")
        if not row_results:
            for i, row_data in enumerate(batch_data):
                row_results.append({
                    "row_number_excel": row_data.get('_row_number_excel', i + 1),
                    "status": "error",
                    "message": f"Error en lote: {str(e)}",
                    "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}
                })
            rows_with_errors = len(batch_data)

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    processing_time = time.time() - start_time
    logger.info(f"[BATCH_{batch_id}] Procesadas {len(batch_data)} filas en {processing_time:.2f}s. Insertadas: {rows_inserted}, Errores: {rows_with_errors}")

    return {
        "batch_id": batch_id,
        "inserted": rows_inserted,
        "updated": 0,
        "errors": rows_with_errors,
        "row_details": row_results,
        "processing_time": processing_time
    }

def process_batch_update(batch_data: List[Dict], table_name: str, columns: List[str], 
                        key_columns: List[str], conn_params: Dict, batch_id: int, mode: str) -> Dict:
    """Procesar un lote de datos para actualización/sincronización con manejo de errores a nivel de fila."""
    start_time = time.time()
    conn = None
    cursor = None
    
    row_results = []
    rows_inserted = 0
    rows_updated = 0
    rows_with_errors = 0

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        for i, row_data in enumerate(batch_data):
            row_number = row_data.get('_row_number_excel', i + 1)
            
            try:
                cleaned_row_data = {k: (None if pd.isna(v) else v) for k, v in row_data.items()}

                key_values = [cleaned_row_data.get(key) for key in key_columns]
                if any(v is None for v in key_values):
                    raise ValueError("Las columnas clave no pueden ser nulas.")

                where_clause = ' AND '.join(f'"{key}" = %s' for key in key_columns)
                check_query = f'SELECT 1 FROM "{table_name}" WHERE {where_clause}'
                cursor.execute(check_query, key_values)
                exists = cursor.fetchone() is not None

                if exists:
                    if mode in ['update', 'sync']:
                        non_key_columns = [col for col in columns if col not in key_columns]
                        if non_key_columns:
                            set_clause = ', '.join(f'"{col}" = %s' for col in non_key_columns)
                            update_query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'
                            update_values = [cleaned_row_data.get(col) for col in non_key_columns]
                            cursor.execute(update_query, update_values + key_values)
                            rows_updated += 1
                            row_results.append({"row_number_excel": row_number, "status": "updated", "message": "", "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}})
                        else:
                             row_results.append({"row_number_excel": row_number, "status": "skipped", "message": "Sin cambios", "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}})
                    else:
                        raise ValueError("El registro ya existe (modo inserción).")
                else:
                    if mode in ['insert', 'sync']:
                        columns_str = ', '.join(f'"{col}"' for col in columns)
                        placeholders = ', '.join(['%s'] * len(columns))
                        insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
                        insert_values = [cleaned_row_data.get(col) for col in columns]
                        cursor.execute(insert_query, insert_values)
                        rows_inserted += 1
                        row_results.append({"row_number_excel": row_number, "status": "inserted", "message": "", "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}})
                    else:
                        raise ValueError("El registro no se encontró para actualizar.")

            except Exception as row_error:
                conn.rollback()
                rows_with_errors += 1
                row_results.append({
                    "row_number_excel": row_number,
                    "status": "error",
                    "message": f"Error en fila: {str(row_error).strip()}",
                    "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}
                })
        
        conn.commit()

    except Exception as e:
        logger.error(f"[BATCH_{batch_id}] Error catastrófico en lote de actualización: {e}")
        if not row_results:
            for i, row_data in enumerate(batch_data):
                row_results.append({
                    "row_number_excel": row_data.get('_row_number_excel', i + 1),
                    "status": "error",
                    "message": f"Error en lote: {str(e)}",
                    "data": {k: str(v) for k, v in row_data.items() if k != '_row_number_excel'}
                })
            rows_with_errors = len(batch_data)
    
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    processing_time = time.time() - start_time
    logger.info(f"[BATCH_{batch_id}] Procesadas {len(batch_data)} filas: {rows_inserted} insertadas, {rows_updated} actualizadas, {rows_with_errors} errores en {processing_time:.2f}s")
    
    return {
        "batch_id": batch_id,
        "inserted": rows_inserted,
        "updated": rows_updated,
        "errors": rows_with_errors,
        "row_details": row_results,
        "processing_time": processing_time
    }

def insert_data_to_table_optimized(df: pd.DataFrame, table_name: str, column_mapping: Dict, 
                                 mode: str = 'insert', user_key_columns: List = None, 
                                 fk_mappings: Dict = None, batch_size: int = 1000, 
                                 max_workers: int = 4) -> Dict:
    """Versión optimizada con validación de FK previa y procesamiento por lotes."""
    start_time = time.time()
    logger.info(f"[ETL_OPTIMIZED] Iniciando procesamiento optimizado")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if mode == 'insert':
            clean_table_for_insert(table_name, conn)
        
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s AND table_schema = 'public'", (table_name,))
        table_columns = {row[0] for row in cursor.fetchall()}
        
        df_mapped = df.copy()
        
        if column_mapping:
            reverse_mapping = {v: k for k, v in column_mapping.items()}
            valid_mapped_columns = {source_col: target_col for target_col, source_col in reverse_mapping.items() if target_col in table_columns and source_col in df_mapped.columns}
            if not valid_mapped_columns:
                raise HTTPException(status_code=400, detail="No hay columnas válidas para mapear")
            df_final = df_mapped[list(valid_mapped_columns.keys())].rename(columns=valid_mapped_columns)
        else:
            valid_columns = [col for col in df_mapped.columns if col in table_columns]
            if not valid_columns:
                raise HTTPException(status_code=400, detail="No hay columnas que coincidan con la tabla destino")
            df_final = df_mapped[valid_columns]
        
        if df_final.empty:
            return {"inserted": 0, "updated": 0, "errors": 0, "total": 0, "row_details": [], "processing_time": time.time() - start_time}

        # --- VALIDACIÓN DE FK ANTES DE PROCESAR ---
        fk_error_row_details = []
        df_to_process = df_final
        
        if fk_mappings:
            logger.info(f"Resolviendo y validando FKs para {len(df_final)} filas...")
            df_resolved = resolve_foreign_keys_batch(df_final.copy(), fk_mappings, conn)
            
            fk_columns = fk_mappings.keys()
            invalid_fk_mask = df_resolved[fk_columns].isnull().any(axis=1)
            
            df_invalid_fk = df_final[invalid_fk_mask]
            df_to_process = df_resolved[~invalid_fk_mask]
            
            for index, row in df_invalid_fk.iterrows():
                failed_cols_msg = [f"{col} (valor: '{row[col]}')" for col in fk_columns if pd.isna(df_resolved.loc[index, col])]
                error_msg = f"Error de FK: No se encontró el valor correspondiente para: {', '.join(failed_cols_msg)}"
                fk_error_row_details.append({
                    "row_number_excel": index + 2, "status": "error", "message": error_msg,
                    "data": {k: str(v) for k, v in row.to_dict().items()}
                })
            logger.info(f"Validación de FK: {len(df_invalid_fk)} filas inválidas, {len(df_to_process)} para procesar.")

        if df_to_process.empty:
            return {
                "inserted": 0, "updated": 0, "errors": len(fk_error_row_details), "total": 0,
                "row_details": fk_error_row_details, "processing_time": time.time() - start_time,
                "batches_processed": 0, "optimization_used": True
            }

        # --- PROCESAMIENTO POR LOTES (SOLO DATOS VÁLIDOS) ---
        key_columns = []
        if mode in ['update', 'sync']:
            key_columns = [col['name'] for col in user_key_columns if col['name'] in df_to_process.columns] if user_key_columns else [col for col in df_to_process.columns if any(k in col.lower() for k in ['id', 'cedula', 'codigo'])]
            if not key_columns:
                raise HTTPException(status_code=400, detail="No se encontraron columnas clave para el modo de actualización/sincronización.")

        columns = list(df_to_process.columns)
        data_list = [dict(row, _row_number_excel=index + 2) for index, row in df_to_process.iterrows()]
        batches = [data_list[i:i + batch_size] for i in range(0, len(data_list), batch_size)]
        
        all_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # ... (lógica de ejecución de futuros como antes) ...
            future_to_batch = {
                executor.submit(
                    process_batch_insert if mode == 'insert' else process_batch_update,
                    batch_data, table_name, columns, 
                    *( (DB_CONFIG, batch_id) if mode == 'insert' else (key_columns, DB_CONFIG, batch_id, mode) )
                ): batch_id
                for batch_id, batch_data in enumerate(batches)
            }
            for future in as_completed(future_to_batch):
                try:
                    all_results.append(future.result())
                except Exception as e:
                    batch_id = future_to_batch[future]
                    logger.error(f"[ETL_OPTIMIZED] Error en futuro de lote {batch_id}: {e}")

        # --- CONSOLIDACIÓN DE RESULTADOS ---
        total_inserted = sum(r.get("inserted", 0) for r in all_results)
        total_updated = sum(r.get("updated", 0) for r in all_results)
        processing_errors = sum(r.get("errors", 0) for r in all_results)
        
        processing_row_details = [detail for r in all_results for detail in r.get("row_details", [])]
        all_row_details = sorted(fk_error_row_details + processing_row_details, key=lambda x: x["row_number_excel"])
        
        cursor.close()
        conn.close()
        
        return {
            "inserted": total_inserted, "updated": total_updated,
            "errors": len(fk_error_row_details) + processing_errors,
            "total": total_inserted + total_updated,
            "row_details": all_row_details,
            "processing_time": time.time() - start_time,
            "batches_processed": len(batches), "optimization_used": True
        }
        
    except Exception as e:
        logger.error(f"[ETL_OPTIMIZED] Error en procesamiento optimizado: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=f"Error en procesamiento optimizado: {str(e)}")

# La función secuencial insert_data_to_table se omite por brevedad, ya que el flujo principal usa la optimizada.
# En una implementación completa, también se refactorizaría para seguir la misma lógica de validación previa.
def insert_data_to_table(df: pd.DataFrame, table_name: str, column_mapping: Dict, mode: str = 'insert', user_key_columns: List = None, fk_mappings: Dict = None) -> Dict:
    # Esta función ahora delega a la versión optimizada para mantener la consistencia.
    logger.info("Redirigiendo a la implementación optimizada para el procesamiento secuencial.")
    return insert_data_to_table_optimized(df, table_name, column_mapping, mode, user_key_columns, fk_mappings, batch_size=len(df), max_workers=1)
