from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Depends
from fastapi.responses import FileResponse
import uuid
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import psycopg2
from models import ETLSession, ETLConfig
from database import get_db_connection, get_database_tables, get_table_columns_info, get_foreign_table_columns_info
from file_utils import detect_file_type, read_excel_sheets, read_file_data
from transformations import apply_transformations
from processing import insert_data_to_table, insert_data_to_table_optimized, get_mode_description_for_report
from config import UPLOAD_DIR

import logging
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/etl",
    tags=["ETL"]
)

# Almacenamiento en memoria para sesiones ETL
etl_sessions: Dict[str, ETLSession] = {}

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Endpoint para cargar archivo"""
    try:
        session_id = str(uuid.uuid4())
        file_extension = file.filename.split('.')[-1]
        temp_filename = f"{session_id}.{file_extension}"
        file_path = UPLOAD_DIR / temp_filename
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        file_type = detect_file_type(file.filename)
        
        session = ETLSession(session_id)
        session.file_path = str(file_path)
        session.file_type = file_type
        
        if file_type == 'excel':
            session.sheets = read_excel_sheets(str(file_path))
        else:
            session.sheets = ['default']
        
        etl_sessions[session_id] = session
        
        return {
            "session_id": session_id,
            "file_id": session_id,
            "file_type": file_type,
            "sheets": session.sheets
        }
    except Exception as e:
        logger.error(f"Error en upload: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/preview")
async def get_preview(session_id: str = Form(...), sheet: str = Form(...)):
    """Endpoint para obtener vista previa de datos"""
    try:
        if session_id not in etl_sessions:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
        
        session = etl_sessions[session_id]
        
        sheet_name = sheet if sheet != 'default' else None
        df = read_file_data(session.file_path, session.file_type, sheet_name)
        
        session.dataframe = df
        session.selected_sheet = sheet
        session.columns = list(df.columns)
        
        preview_data = df.head(5).fillna('').to_dict('records')
        session.preview_data = preview_data
        
        return {
            "columns": session.columns,
            "preview_data": preview_data,
            "total_rows": len(df)
        }
    except Exception as e:
        logger.error(f"Error en preview: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/tables")
async def get_tables():
    """Endpoint para obtener tablas disponibles"""
    try:
        tables = get_database_tables()
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error obteniendo tablas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/columns/{table_name}")
async def get_table_columns(table_name: str):
    """Endpoint para obtener columnas de una tabla específica"""
    try:
        columns = get_table_columns_info(table_name)
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/foreign-table-columns/{table_name}")
async def get_foreign_table_columns(table_name: str):
    """Endpoint para obtener columnas de una tabla relacionada por foreign key"""
    try:
        columns = get_foreign_table_columns_info(table_name)
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla relacionada {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/config/save")
async def save_config(config: ETLConfig):
    """Guardar configuración de ETL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO etl_configs (name, description, config_data)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE 
            SET description = EXCLUDED.description,
                config_data = EXCLUDED.config_data
        """, (config.name, config.description, json.dumps(config.dict())))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Configuración guardada exitosamente"}
    except Exception as e:
        logger.error(f"Error guardando configuración: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/configs")
async def get_configs():
    """Obtener lista de configuraciones de ETL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("SELECT name, description, created_at FROM etl_configs ORDER BY created_at DESC")
        configs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {"configs": configs}
    except Exception as e:
        logger.error(f"Error obteniendo configuraciones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/{name}")
async def get_config(name: str):
    """Obtener configuración de ETL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("SELECT * FROM etl_configs WHERE name = %s", (name,))
        config = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not config:
            raise HTTPException(status_code=404, detail="Configuración no encontrada")
        
        return config
    except Exception as e:
        logger.error(f"Error obteniendo configuración: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process")
async def process_data(
    session_id: str = Form(...),
    sheet: str = Form(...),
    column_mapping: str = Form(...),
    transformations: str = Form(...),
    target_table: str = Form(...),
    mode: str = Form(default='insert'),
    encoding: str = Form(default='latin1'),
    key_columns: str = Form(default='[]'),
    fk_mappings: str = Form(default='{}'),
    config_name: Optional[str] = Form(default=None),
    use_optimization: bool = Form(default=True),
    batch_size: int = Form(default=1000),
    max_workers: int = Form(default=4),
    compare_column: Optional[str] = Form(default=None),
    delete_missing: bool = Form(default=False),
    load_missing: bool = Form(default=False)
):
    """Endpoint para procesar y cargar datos"""
    logger.info("[ETL_ENDPOINT] INICIO DEL PROCESAMIENTO ETL")
    
    try:
        if session_id not in etl_sessions:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
        
        session = etl_sessions[session_id]
        
        column_mapping_dict = json.loads(column_mapping) if column_mapping else {}
        transformations_dict = json.loads(transformations) if transformations else {}
        key_columns_list = json.loads(key_columns) if key_columns else []
        fk_mappings_dict = json.loads(fk_mappings) if fk_mappings else {}
        
        df = session.dataframe
        if df is None:
            raise HTTPException(status_code=400, detail="No hay datos cargados")
        
        df_transformed = apply_transformations(df, transformations_dict)
        
        if use_optimization and len(df_transformed) >= 100:
            result = insert_data_to_table_optimized(
                df_transformed, target_table, column_mapping_dict, mode, 
                key_columns_list, fk_mappings_dict, batch_size, max_workers
            )
        else:
            result = insert_data_to_table(
                df_transformed, target_table, column_mapping_dict, mode, 
                key_columns_list, fk_mappings_dict
            )
        
        # Generar informe detallado en TXT
        optimization_info = ""
        if result.get("optimization_used", False):
            processing_time = result.get("processing_time", 0)
            batches_processed = result.get("batches_processed", 0)
            optimization_info = f"""
Optimización Aplicada:
  - Procesamiento por Lotes: SÍ
  - Multihilos: SÍ
  - Tamaño de Lote: {batch_size} filas
  - Hilos Utilizados: {max_workers}
  - Lotes Procesados: {batches_processed}
  - Tiempo Total: {processing_time:.2f} segundos
  - Velocidad: {round(result["total"] / processing_time if processing_time > 0 else 0, 2)} filas/segundo
------------------------------------"""
        else:
            optimization_info = """
Optimización Aplicada: NO (Procesamiento secuencial)
------------------------------------"""

        report_content = f"""
--- Informe de Procesamiento ETL ---
Fecha y Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
ID de Sesión: {session_id}
Tabla Destino: {target_table}
Modo de Procesamiento: {mode} ({get_mode_description_for_report(mode)})
{optimization_info}

Estadísticas:
  - Total de Filas Procesadas: {result["total"]}
  - Filas Insertadas: {result["inserted"]}
  - Filas Actualizadas: {result["updated"]}
  - Filas con Errores: {result["errors"]}
  - Tasa de Éxito: {round((result["total"] / (result["total"] + result["errors"]) * 100), 2) if (result["total"] + result["errors"]) > 0 else 0}%
------------------------------------

Detalles por Fila:
"""
        
        for row_detail in result.get("row_details", []):
            report_content += f"""
  Fila Excel: {row_detail["row_number_excel"]}
  Estado: {row_detail["status"].capitalize()}
  Mensaje: {row_detail["message"] if row_detail["message"] else "N/A"}
  Datos Clave: {json.dumps(row_detail["data"], ensure_ascii=False, indent=2)}
------------------------------------
"""
        
        report_filename = f"etl_report_{session_id}.txt"
        report_path = Path("logs") / report_filename
        
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report_content)
            logger.info(f"Informe generado en: {report_path}")
        except Exception as report_error:
            logger.error(f"Error al generar el informe TXT: {report_error}")
            report_path = None

        if os.path.exists(session.file_path):
            os.remove(session.file_path)
        
        if session_id in etl_sessions:
            del etl_sessions[session_id]
        
        return {
            "success": True,
            "result": result,
            "target_table": target_table,
            "mode": mode,
            "report": {
                "total_rows": result["total"],
                "inserted_rows": result["inserted"],
                "updated_rows": result["updated"],
                "error_rows": result["errors"],
                "success_rate": round((result["total"] / (result["total"] + result["errors"]) * 100), 2) if (result["total"] + result["errors"]) > 0 else 0
            },
            "report_file": str(report_path) if report_path else None
        }
    except Exception as e:
        logger.error(f"Error procesando datos: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download-report/{filename}")
async def download_report(filename: str):
    """Endpoint para descargar reportes generados"""
    try:
        report_path = Path("logs") / filename
        if not report_path.exists():
            raise HTTPException(status_code=404, detail="Archivo de reporte no encontrado")
        
        return FileResponse(
            path=str(report_path),
            filename=filename,
            media_type='text/plain'
        )
    except Exception as e:
        logger.error(f"Error descargando reporte: {e}")
        raise HTTPException(status_code=500, detail="Error descargando reporte")
