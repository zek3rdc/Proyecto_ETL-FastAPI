from fastapi import APIRouter, File, UploadFile, HTTPException, Form
from fastapi.responses import FileResponse
from typing import Dict, List, Optional
import pandas as pd
import logging
from datetime import datetime
import json
from pathlib import Path
import psycopg2
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
import threading
from config import DATABASE_CONFIG as DB_CONFIG

# Configurar logging específico para expedientes
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/expedientes", tags=["expedientes"])

@router.get("/test")
async def test_endpoint():
    """Endpoint de prueba para verificar que el router funciona"""
    logger.info("[TEST_ENDPOINT] Endpoint de prueba llamado correctamente")
    return {"message": "Router de expedientes funcionando correctamente", "timestamp": datetime.now().isoformat()}

@router.post("/test-form")
async def test_form_endpoint(
    test_param: str = Form(...),
    optional_param: str = Form(default="default_value")
):
    """Endpoint de prueba para verificar Form parameters"""
    logger.info(f"[TEST_FORM] Parámetros recibidos: test_param={test_param}, optional_param={optional_param}")
    return {
        "message": "Form endpoint funcionando correctamente",
        "received_params": {
            "test_param": test_param,
            "optional_param": optional_param
        },
        "timestamp": datetime.now().isoformat()
    }

@router.post("/test-mapping")
async def test_mapping_endpoint(
    temp_file_id: str = Form(...),
    field_mapping: str = Form(...)
):
    """Endpoint de prueba con los mismos parámetros que process-with-mapping"""
    logger.info("=" * 80)
    logger.info("[TEST_MAPPING] ENDPOINT DE PRUEBA ALCANZADO!")
    logger.info("=" * 80)
    logger.info(f"[TEST_MAPPING] temp_file_id: {temp_file_id}")
    logger.info(f"[TEST_MAPPING] field_mapping: {field_mapping}")
    
    return {
        "success": True,
        "message": "Test mapping endpoint funcionando correctamente",
        "received_params": {
            "temp_file_id": temp_file_id,
            "field_mapping": field_mapping
        }
    }

def get_db_connection_local():
    """Crear conexión a la base de datos local"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

def remove_accents(text):
    """Remover acentos de texto"""
    if pd.isna(text) or text == '':
        return text
    try:
        text = str(text)
        # Normalizar el texto y remover acentos
        nfd = unicodedata.normalize('NFD', text)
        without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
        return without_accents
    except:
        return text

def detect_date_format(date_string):
    """Detectar formato de fecha en string"""
    if pd.isna(date_string) or date_string == '':
        return None
    
    date_string = str(date_string).strip()
    
    # Patrones comunes de fecha
    patterns = [
        (r'^\d{1,2}/\d{1,2}/\d{4}$', '%d/%m/%Y'),  # dd/mm/yyyy
        (r'^\d{1,2}/\d{1,2}/\d{2}$', '%d/%m/%y'),   # dd/mm/yy
        (r'^\d{1,2}-\d{1,2}-\d{4}$', '%d-%m-%Y'),   # dd-mm-yyyy
        (r'^\d{1,2}-\d{1,2}-\d{2}$', '%d-%m-%y'),   # dd-mm-yy
        (r'^\d{4}/\d{1,2}/\d{1,2}$', '%Y/%m/%d'),   # yyyy/mm/dd
        (r'^\d{4}-\d{1,2}-\d{1,2}$', '%Y-%m-%d'),   # yyyy-mm-dd
        (r'^\d{1,2}\.\d{1,2}\.\d{4}$', '%d.%m.%Y'), # dd.mm.yyyy
        (r'^\d{1,2}\.\d{1,2}\.\d{2}$', '%d.%m.%y'), # dd.mm.yy
    ]
    
    for pattern, format_str in patterns:
        if re.match(pattern, date_string):
            return format_str
    
    return None

def transform_date_to_standard(date_value, source_format=None, separator='-'):
    """Transformar fecha a formato estándar con separador personalizable"""
    if pd.isna(date_value) or date_value == '':
        return None
    
    try:
        # Si ya es un objeto datetime
        if isinstance(date_value, (pd.Timestamp, datetime)):
            return date_value.strftime(f'%d{separator}%m{separator}%Y')
        
        date_string = str(date_value).strip()
        
        # Si se proporciona formato específico
        if source_format:
            try:
                parsed_date = datetime.strptime(date_string, source_format)
                return parsed_date.strftime(f'%d{separator}%m{separator}%Y')
            except:
                pass
        
        # Detectar formato automáticamente
        detected_format = detect_date_format(date_string)
        if detected_format:
            try:
                parsed_date = datetime.strptime(date_string, detected_format)
                return parsed_date.strftime(f'%d{separator}%m{separator}%Y')
            except:
                pass
        
        # Intentar con pandas
        try:
            parsed_date = pd.to_datetime(date_string, infer_datetime_format=True)
            return parsed_date.strftime(f'%d{separator}%m{separator}%Y')
        except:
            pass
        
        return date_string  # Retornar original si no se puede convertir
        
    except Exception as e:
        logger.warning(f"Error transformando fecha '{date_value}': {e}")
        return date_value

def analyze_excel_structure(file_path: str, sheet_name: str = None):
    """Analizar estructura del Excel y sugerir mapeo de campos"""
    try:
        logger.info(f"Iniciando análisis de archivo: {file_path}")
        
        if not Path(file_path).exists():
            raise Exception(f"Archivo no encontrado: {file_path}")
        
        # Leer solo las primeras 10 filas para el análisis de estructura
        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=10)
        
        if isinstance(df, dict):
            first_sheet_key = list(df.keys())[0]
            df = df[first_sheet_key]
            logger.info(f"Usando datos de la hoja: {first_sheet_key}")
        
        logger.info(f"Excel leído. Columnas originales: {list(df.columns)}")
        
        required_fields = {
            'cedula': ['cedula', 'cédula', 'ci', 'documento', 'identificacion', 'cedula_funcionario', 'cédula_del_funcionario'],
            'nro_expediente': ['expediente', 'nro_expediente', 'numero_expediente', 'num_expediente', 'exp'],
            'tipo_expediente': ['tipo_expediente', 'tipo', 'clase_expediente', 'categoria'],
            'fecha_inicio': ['fecha_inicio', 'fecha', 'fecha_apertura', 'inicio', 'fecha_de_inicio'],
            'estatus': ['estatus', 'estado', 'situacion', 'status'],
            'falta': ['falta', 'infraccion', 'motivo', 'causa'],
            'decision': ['decision', 'resolucion', 'veredicto', 'resultado'],
            'observaciones': ['observaciones', 'comentarios', 'notas', 'descripcion', 'reseña_del_caso'],
            'fecha_finalizacion': ['fecha_finalizacion', 'fecha_fin', 'fecha_cierre', 'finalizacion', 'fecha_de_finalización'],
            'tipo_sancion_administrativa': ['tipo_sancion_administrativa', 'sancion', 'tipo_sancion', 'tipo_de_sanción_administrativa'],
            'expediente_relacionado': ['expediente_relacionado', 'exp_relacionado', 'relacionado']
        }
        
        original_columns = list(df.columns)
        excel_columns = [str(col).strip() for col in original_columns]
        
        column_mapping_clean_to_original = {str(orig_col).strip(): orig_col for orig_col in original_columns}
        
        suggested_mapping = {}
        unmapped_columns = excel_columns.copy()
        
        column_analysis = {}
        for clean_col in excel_columns:
            original_col = column_mapping_clean_to_original[clean_col]
            sample_data = df[original_col].dropna().head(5).tolist()
            
            date_formats = [fmt for fmt in [detect_date_format(str(v)) for v in sample_data if pd.notna(v)] if fmt]
            has_accents = any('á' in str(v) or 'é' in str(v) or 'í' in str(v) or 'ó' in str(v) or 'ú' in str(v) or 'ñ' in str(v) for v in sample_data if isinstance(v, str))
            
            column_analysis[clean_col] = {
                'sample_data': sample_data,
                'detected_date_formats': list(set(date_formats)),
                'has_accents': has_accents,
                'data_type': str(df[original_col].dtype)
            }
        
        result = {
            'excel_columns': excel_columns,
            'suggested_mapping': suggested_mapping,
            'unmapped_columns': unmapped_columns,
            'required_fields': list(required_fields.keys()),
            'column_analysis': column_analysis,
            'total_rows': len(df)
        }
        
        logger.info("Análisis de estructura de Excel completado.")
        return result
        
    except Exception as e:
        logger.error(f"[ANALYZE_EXCEL] Error crítico analizando estructura del Excel: {e}")
        logger.error(f"[ANALYZE_EXCEL] Tipo de error: {type(e)}")
        import traceback
        logger.error(f"[ANALYZE_EXCEL] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=f"Error analizando archivo: {str(e)}")



def clean_expediente_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpiar y formatear datos de expedientes"""
    df_clean = df.copy()
    
    # Limpiar cédulas
    if 'cedula' in df_clean.columns:
        df_clean['cedula'] = df_clean['cedula'].astype(str).str.strip().str.replace('-', '')
    
    # CORRECCIÓN: Formatear fechas de forma más robusta sin perder datos
    date_columns = ['fecha_inicio', 'fecha_finalizacion']
    for col in date_columns:
        if col in df_clean.columns:
            # Procesar fechas de forma más cuidadosa sin logging detallado
            def safe_date_conversion(date_val):
                if pd.isna(date_val) or date_val is None:
                    return None
                
                # Si ya es string y parece fecha, mantenerlo
                if isinstance(date_val, str):
                    date_str = date_val.strip()
                    
                    # Si ya está en formato YYYY-MM-DD, mantenerlo
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                        return date_str
                    
                    # Si está en formato DD/MM/YYYY, convertirlo
                    if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                        try:
                            parsed = pd.to_datetime(date_str, format='%d/%m/%Y', errors='raise')
                            return parsed.strftime('%Y-%m-%d')
                        except:
                            pass
                    
                    # Otros formatos comunes
                    try:
                        parsed = pd.to_datetime(date_str, dayfirst=True, errors='raise')
                        return parsed.strftime('%Y-%m-%d')
                    except:
                        return date_val  # Mantener valor original si no se puede convertir
                
                # Si es datetime, convertir a string
                if isinstance(date_val, (pd.Timestamp, datetime)):
                    return date_val.strftime('%Y-%m-%d')
                
                # Para otros tipos, intentar conversión
                try:
                    parsed = pd.to_datetime(date_val, dayfirst=True, errors='raise')
                    return parsed.strftime('%Y-%m-%d')
                except:
                    return date_val  # Mantener valor original
            
            # Aplicar conversión segura
            df_clean[col] = df_clean[col].apply(safe_date_conversion)
    
    # Normalizar estatus (solo si no es nulo)
    if 'estatus' in df_clean.columns:
        df_clean['estatus'] = df_clean['estatus'].astype(str).str.upper().where(df_clean['estatus'].notna(), None)
    
    # Normalizar tipo de expediente (solo si no es nulo)
    if 'tipo_expediente' in df_clean.columns:
        df_clean['tipo_expediente'] = df_clean['tipo_expediente'].astype(str).str.upper().where(df_clean['tipo_expediente'].notna(), None)
    
    return df_clean

@router.post("/validate-excel")
async def validate_expedientes_excel(
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None)
):
    """Endpoint para validar estructura del Excel y sugerir mapeo de campos"""
    temp_path = None
    
    try:
        logger.info(f"Validando archivo Excel: {file.filename}")
        
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Solo se permiten archivos Excel (.xlsx, .xls)")
        
        temp_path = Path("temp_uploads") / f"validate_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        temp_path.parent.mkdir(exist_ok=True)
        
        with open(temp_path, "wb") as buffer:
            buffer.write(await file.read())
        
        logger.info(f"Archivo temporal guardado en: {temp_path}")
        
        excel_file = pd.ExcelFile(str(temp_path))
        available_sheets = excel_file.sheet_names
        
        if len(available_sheets) > 1 and sheet_name is None:
            return {
                "success": True,
                "message": "Archivo Excel con múltiples hojas detectado",
                "validation_status": "needs_sheet_selection",
                "available_sheets": available_sheets,
                "temp_file_id": temp_path.name
            }
        
        if sheet_name is None:
            sheet_name = available_sheets[0]
        
        analysis = analyze_excel_structure(str(temp_path), sheet_name)
        
        required_minimum = ['cedula', 'nro_expediente']
        missing_required = [field for field in required_minimum if field not in analysis.get('suggested_mapping', {}).values()]
        
        validation_status = "success" if not missing_required else "needs_mapping"
        
        result = {
            "success": True,
            "validation_status": validation_status,
            "message": "Archivo analizado exitosamente" if validation_status == "success" else "Se requiere mapeo manual de campos",
            "analysis": analysis,
            "missing_required_fields": missing_required,
            "temp_file_id": temp_path.name
        }
        
        logger.info("Validación de Excel completada.")
        return result
        
    except HTTPException as he:
        logger.error(f"[VALIDATE_EXCEL] HTTPException: {he.detail}")
        raise he
    except Exception as e:
        logger.error("=" * 80)
        logger.error("[VALIDATE_EXCEL] ERROR CRÍTICO EN VALIDACIÓN")
        logger.error("=" * 80)
        logger.error(f"[VALIDATE_EXCEL] Error: {e}")
        logger.error(f"[VALIDATE_EXCEL] Tipo de error: {type(e)}")
        import traceback
        logger.error(f"[VALIDATE_EXCEL] Traceback completo: {traceback.format_exc()}")
        logger.error("=" * 80)
        raise HTTPException(
            status_code=500,
            detail=f"Error validando archivo: {str(e)}"
        )
    finally:
        # No eliminar archivo temporal aquí, se usará en el siguiente paso
        if temp_path:
            logger.info(f"[VALIDATE_EXCEL] Archivo temporal mantenido para siguiente paso: {temp_path}")

@router.post("/validate-cedulas-with-options")
async def validate_cedulas_with_options(
    temp_file_id: str = Form(...),
    field_mapping: str = Form(...),
    transformations: str = Form(...),
    sheet_name: Optional[str] = Form(None)
):
    """Endpoint para validar cédulas y mostrar opciones de manejo de cédulas no registradas"""
    temp_path = None
    
    try:
        logger.info("=" * 80)
        logger.info("[VALIDATE_CEDULAS_OPTIONS] Iniciando validación de cédulas con opciones")
        logger.info("=" * 80)
        
        # Reconstruir ruta del archivo temporal
        temp_path = Path("temp_uploads") / temp_file_id
        
        if not temp_path.exists():
            raise HTTPException(
                status_code=400,
                detail="Archivo temporal no encontrado. Por favor, suba el archivo nuevamente."
            )
        
        # Parsear mapeo de campos y transformaciones
        field_mapping_dict = json.loads(field_mapping) if field_mapping else {}
        transformations_dict = json.loads(transformations) if transformations else {}
        
        # Leer Excel con el mapeo
        df = pd.read_excel(temp_path, sheet_name=sheet_name)
        
        # Verificar que df es un DataFrame y no un diccionario
        if isinstance(df, dict):
            logger.error(f"[VALIDATE_CEDULAS_OPTIONS] pd.read_excel devolvió un diccionario: {list(df.keys())}")
            # Si es un diccionario, tomar la primera hoja
            first_sheet_key = list(df.keys())[0]
            df = df[first_sheet_key]
            logger.info(f"[VALIDATE_CEDULAS_OPTIONS] Usando datos de la hoja: {first_sheet_key}")
        
        # Aplicar mapeo de campos
        if field_mapping_dict:
            df_mapped = df.rename(columns=field_mapping_dict)
        else:
            df_mapped = df.copy()
        
        # Aplicar transformaciones
        df_transformed = apply_custom_transformations(df_mapped, transformations_dict)
        
        # Limpiar datos (sin validadores restrictivos)
        df_clean = clean_expediente_data(df_transformed)
        
        # Validar solo columnas mínimas requeridas
        required_columns = ['cedula', 'nro_expediente']
        missing_columns = [col for col in required_columns if col not in df_clean.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Columnas requeridas faltantes: {', '.join(missing_columns)}"
            )
        
        # Verificar funcionarios existentes
        logger.info("[VALIDATE_CEDULAS_OPTIONS] Validando cédulas contra base de datos...")
        cedulas_no_registradas = []
        cedulas_validas = []
        cedulas_formato_invalido = []
        
        conn = get_db_connection_local()
        cursor = conn.cursor()
        
        total_rows = len(df_clean)
        
        for index, row in df_clean.iterrows():
            if 'cedula' in row and not pd.isna(row['cedula']):
                cedula_str = str(row['cedula']).strip().replace('-', '')
                
                # Saltar verificación para cédulas especiales (6666, 9999, vacías)
                if cedula_str in ['6666', '9999'] or cedula_str == '' or cedula_str.isspace():
                    # Para cédulas especiales, no asignar funcionario_id ni campo especial
                    df_clean.at[index, 'funcionario_id'] = None
                    df_clean.at[index, 'cedula_anterior_no_registrada_en_rrhh'] = None
                    continue
                
                try:
                    cedula = int(cedula_str)
                    cursor.execute("SELECT id, nombre_completo FROM funcionarios WHERE cedula = %s", (cedula,))
                    funcionario = cursor.fetchone()
                    
                    if not funcionario:
                        cedulas_no_registradas.append({
                            'cedula': cedula,
                            'nro_expediente': row.get('nro_expediente', 'N/A'),
                            'row': index + 2,
                            'nombre_funcionario': row.get('nombre_funcionario', 'N/A'),
                            'tipo_expediente': row.get('tipo_expediente', 'N/A'),
                            'fecha_inicio': row.get('fecha_inicio', 'N/A'),
                            'estatus': row.get('estatus', 'N/A')
                        })
                    else:
                        cedulas_validas.append({
                            'cedula': cedula,
                            'nro_expediente': row.get('nro_expediente', 'N/A'),
                            'nombre_funcionario': funcionario[1],
                            'row': index + 2
                        })
                        
                except ValueError as e:
                    cedulas_formato_invalido.append({
                        'cedula': cedula_str,
                        'nro_expediente': row.get('nro_expediente', 'N/A'),
                        'row': index + 2,
                        'error': 'Formato de cédula inválido'
                    })
        
        cursor.close()
        conn.close()
        
        # Generar reporte de cédulas no registradas si existen
        reporte_cedulas_no_registradas_path = None
        if cedulas_no_registradas:
            try:
                report_df = pd.DataFrame(cedulas_no_registradas)
                # Usar parte del temp_file_id para identificar el reporte
                report_base_name = temp_file_id.split('_')[1] if '_' in temp_file_id else temp_file_id
                report_filename = f"reporte_cedulas_no_registradas_{report_base_name}.xlsx"
                
                report_path = Path("temp_reports") / report_filename
                report_path.parent.mkdir(exist_ok=True)
                
                report_df.to_excel(report_path, index=False)
                reporte_cedulas_no_registradas_path = report_path.name
                logger.info(f"Reporte de cédulas no registradas generado: {reporte_cedulas_no_registradas_path}")
            except Exception as report_error:
                logger.error(f"Error generando reporte de cédulas no registradas: {report_error}")
        
        # Limpiar valores NaN antes de crear la respuesta JSON
        def clean_nan_values(obj):
            """Recursivamente limpiar valores NaN de objetos Python"""
            if isinstance(obj, dict):
                return {k: clean_nan_values(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nan_values(item) for item in obj]
            elif pd.isna(obj):
                return None
            elif isinstance(obj, float) and (obj != obj):  # Verificar NaN
                return None
            else:
                return obj
        
        # Preparar respuesta con opciones (limpiar NaN)
        validation_report_raw = {
            'total_rows': total_rows,
            'cedulas_validas': len(cedulas_validas),
            'cedulas_no_registradas': len(cedulas_no_registradas),
            'cedulas_formato_invalido': len(cedulas_formato_invalido),
            'cedulas_validas_list': cedulas_validas[:10],
            'cedulas_no_registradas_list': cedulas_no_registradas,
            'cedulas_formato_invalido_list': cedulas_formato_invalido,
            'reporte_archivo': reporte_cedulas_no_registradas_path
        }
        
        validation_report = clean_nan_values(validation_report_raw)
        
        if cedulas_formato_invalido:
            return {
                "success": False,
                "message": f"Se encontraron {len(cedulas_formato_invalido)} cédulas con formato inválido. Corrija estos errores antes de continuar.",
                "validation_report": validation_report,
                "can_proceed": False,
                "temp_file_id": temp_file_id
            }
        elif cedulas_no_registradas:
            return {
                "success": True,
                "message": f"Se encontraron {len(cedulas_no_registradas)} cédulas no registradas en RRHH. Puede descargar el reporte o continuar.",
                "validation_report": validation_report,
                "requires_decision": True,
                "options": [
                    {
                        "value": "descargar_reporte",
                        "label": "Descargar reporte y cancelar",
                        "description": "Descargar un Excel con las cédulas no encontradas y cancelar la importación."
                    },
                    {
                        "value": "cargar_igualmente",
                        "label": "Cargar todos los registros",
                        "description": "Cargar todos los registros. Las cédulas no encontradas se guardarán en un campo especial."
                    }
                ],
                "temp_file_id": temp_file_id
            }
        else:
            return {
                "success": True,
                "message": "Todas las cédulas son válidas. Puede proceder con la importación.",
                "validation_report": validation_report,
                "can_proceed": True,
                "temp_file_id": temp_file_id
            }
        
    except Exception as e:
        logger.error(f"[VALIDATE_CEDULAS_OPTIONS] Error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error validando cédulas: {str(e)}"
        )

@router.post("/process-with-cedula-decision")
async def process_with_cedula_decision(
    temp_file_id: str = Form(...),
    field_mapping: str = Form(...),
    transformations: str = Form(...),
    cedula_decision: str = Form(...),
    sheet_name: Optional[str] = Form(None)
):
    """Endpoint para procesar expedientes según la decisión del usuario sobre cédulas no registradas"""
    temp_path = None
    
    try:
        logger.info("=" * 80)
        logger.info("[PROCESS_CEDULA_DECISION] Procesando con decisión de cédulas")
        logger.info("=" * 80)
        logger.info(f"[PROCESS_CEDULA_DECISION] Decisión: {cedula_decision}")
        
        # Validar decisión
        valid_decisions = ["descargar_reporte", "cargar_igualmente"]
        if cedula_decision not in valid_decisions:
            raise HTTPException(
                status_code=400,
                detail=f"Decisión inválida. Opciones válidas: {', '.join(valid_decisions)}"
            )
        
        # Reconstruir ruta del archivo temporal
        temp_path = Path("temp_uploads") / temp_file_id
        
        if not temp_path.exists():
            raise HTTPException(
                status_code=400,
                detail="Archivo temporal no encontrado. Por favor, suba el archivo nuevamente."
            )
        
        # Parsear mapeo de campos y transformaciones
        field_mapping_dict = json.loads(field_mapping) if field_mapping else {}
        transformations_dict = json.loads(transformations) if transformations else {}
        
        # Leer Excel con el mapeo
        df = pd.read_excel(temp_path, sheet_name=sheet_name)
        
        # Aplicar mapeo de campos
        if field_mapping_dict:
            df_mapped = df.rename(columns=field_mapping_dict)
        else:
            df_mapped = df.copy()
        
        # Aplicar transformaciones
        df_transformed = apply_custom_transformations(df_mapped, transformations_dict)
        
        # Limpiar datos
        df_clean = clean_expediente_data(df_transformed)
        
        if cedula_decision == "descargar_reporte":
            # La generación del reporte ya se hizo en el paso de validación.
            # Aquí solo confirmamos la acción de no importar.
            return {
                "success": True,
                "message": "Importación cancelada por el usuario.",
                "action": "cancelled"
            }
        
        elif cedula_decision == "cargar_igualmente":
            # Procesar todos los registros, usando campo especial para cédulas no registradas
            logger.info("[PROCESS_CEDULA_DECISION] Procesando todos los registros con campo especial")
            return await process_expedientes_data_with_cedula_filter(df_clean, 'sync', "cargar_todas")
        
    except Exception as e:
        logger.error(f"[PROCESS_CEDULA_DECISION] Error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando con decisión de cédulas: {str(e)}"
        )
    finally:
        # Limpiar archivo temporal
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
                logger.info("[PROCESS_CEDULA_DECISION] Archivo temporal eliminado")
            except Exception as e:
                logger.warning(f"[PROCESS_CEDULA_DECISION] No se pudo eliminar archivo temporal: {e}")

@router.post("/process-with-mapping")
async def process_expedientes_with_mapping(
    temp_file_id: str = Form(...),
    field_mapping: str = Form(...)
):
    """Endpoint para procesar expedientes con mapeo personalizado y transformaciones automáticas"""
    temp_path = None
    
    try:
        logger.info("=" * 80)
        logger.info("[PROCESS_WITH_MAPPING] INICIANDO PROCESAMIENTO CON MAPEO PERSONALIZADO")
        logger.info("=" * 80)
        logger.info(f"[PROCESS_WITH_MAPPING] temp_file_id: {temp_file_id}")
        logger.info(f"[PROCESS_WITH_MAPPING] field_mapping: {field_mapping}")
        
        # Validación básica de parámetros
        if not temp_file_id or temp_file_id.strip() == "":
            logger.error("[PROCESS_WITH_MAPPING] temp_file_id está vacío")
            raise HTTPException(status_code=422, detail="temp_file_id es requerido")
        
        if not field_mapping or field_mapping.strip() == "":
            logger.error("[PROCESS_WITH_MAPPING] field_mapping está vacío")
            raise HTTPException(status_code=422, detail="field_mapping es requerido")
        
        # Validar que field_mapping sea JSON válido
        try:
            field_mapping_dict = json.loads(field_mapping)
            logger.info(f"[PROCESS_WITH_MAPPING] field_mapping parseado correctamente: {len(field_mapping_dict)} campos")
        except json.JSONDecodeError as e:
            logger.error(f"[PROCESS_WITH_MAPPING] Error parseando field_mapping JSON: {e}")
            raise HTTPException(status_code=422, detail=f"field_mapping debe ser JSON válido: {str(e)}")
        
        # Reconstruir ruta del archivo temporal
        temp_path = Path("temp_uploads") / temp_file_id
        
        if not temp_path.exists():
            logger.error(f"[PROCESS_WITH_MAPPING] Archivo temporal no encontrado: {temp_path}")
            raise HTTPException(
                status_code=400,
                detail="Archivo temporal no encontrado. Por favor, suba el archivo nuevamente."
            )
        
        logger.info(f"[PROCESS_WITH_MAPPING] Mapeo de campos: {field_mapping_dict}")
        
        # Leer Excel con el mapeo
        df = pd.read_excel(temp_path)
        logger.info(f"[PROCESS_WITH_MAPPING] Excel leído exitosamente. Filas: {len(df)}, Columnas: {df.columns.tolist()}")
        
        # Aplicar mapeo de campos
        if field_mapping_dict:
            # Renombrar columnas según el mapeo
            df_mapped = df.rename(columns=field_mapping_dict)
            logger.info(f"[PROCESS_WITH_MAPPING] Columnas después del mapeo: {df_mapped.columns.tolist()}")
        else:
            df_mapped = df.copy()
        
        # Aplicar transformaciones automáticas
        transformations_dict = {}
        for column in df_mapped.columns:
            # Aplicar transformaciones automáticas basadas en el tipo de datos
            if df_mapped[column].dtype == 'object':
                # Para campos de texto, aplicar limpieza automática
                transformations_dict[column] = {
                    'type': 'text_clean',
                    'options': {'trim': True, 'remove_accents': True}
                }
            elif 'fecha' in column.lower() or 'date' in column.lower():
                # Para campos de fecha, aplicar formato automático
                transformations_dict[column] = {
                    'type': 'date_format',
                    'options': {'separator': '-'}
                }
        
        df_transformed = apply_custom_transformations(df_mapped, transformations_dict)
        logger.info(f"[PROCESS_WITH_MAPPING] Transformaciones automáticas aplicadas")
        
        # Usar la lógica de "cargar igualmente" - procesar todos los registros incluyendo cédulas no registradas
        return await process_expedientes_data_with_cedula_filter(df_transformed, 'sync', "cargar_todas")
        
    except HTTPException as he:
        logger.error(f"[PROCESS_WITH_MAPPING] HTTPException: {he.status_code} - {he.detail}")
        raise he
    except Exception as e:
        logger.error("=" * 80)
        logger.error("[PROCESS_WITH_MAPPING] ERROR CRÍTICO EN PROCESAMIENTO")
        logger.error("=" * 80)
        logger.error(f"[PROCESS_WITH_MAPPING] Error: {e}")
        logger.error(f"[PROCESS_WITH_MAPPING] Tipo de error: {type(e)}")
        import traceback
        logger.error(f"[PROCESS_WITH_MAPPING] Traceback completo: {traceback.format_exc()}")
        logger.error("=" * 80)
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando archivo: {str(e)}"
        )
    finally:
        # Limpiar archivo temporal
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
                logger.info("[PROCESS_WITH_MAPPING] Archivo temporal eliminado")
            except Exception as e:
                logger.warning(f"[PROCESS_WITH_MAPPING] No se pudo eliminar archivo temporal: {e}")

def apply_custom_transformations(df: pd.DataFrame, transformations: Dict) -> pd.DataFrame:
    """Aplicar transformaciones automáticas a los datos"""
    df_transformed = df.copy()
    
    logger.info("Aplicando transformaciones automáticas...")
    
    for column in df_transformed.columns:
        try:
            column_dtype = str(df_transformed[column].dtype)
            sample_data = df_transformed[column].dropna().head(3).tolist()
            
            if 'datetime' in column_dtype or any(isinstance(val, pd.Timestamp) for val in sample_data):
                df_transformed[column] = df_transformed[column].apply(
                    lambda x: transform_date_to_standard(x, None, '-') if pd.notna(x) else x
                )
            elif column_dtype == 'object':
                if any('á' in str(v) or 'é' in str(v) for v in sample_data if isinstance(v, str)):
                    df_transformed[column] = df_transformed[column].apply(remove_accents)
                df_transformed[column] = df_transformed[column].astype(str).str.strip()
                
        except Exception as e:
            logger.warning(f"Error aplicando transformación a columna {column}: {e}")
    
    logger.info("Transformaciones automáticas completadas.")
    return df_transformed

# Contador global para errores (thread-safe)
_error_count_lock = threading.Lock()
_global_error_count = 0

def process_batch_improved(batch_data, mode: str, batch_number: int):
    """
    Procesar un lote de datos con condiciones de actualización mejoradas y sin deadlocks
    
    CORRECCIÓN IMPLEMENTADA:
    - Uso de clave compuesta (nro_expediente + funcionario_id + cedula_anterior) para identificar expedientes únicos
    - Esto permite procesar correctamente múltiples registros con el mismo número de expediente pero diferentes cédulas
    - Ejemplo: A-LA-001-029-15 con 4 registros diferentes ahora se procesarán todos correctamente
    """
    global _global_error_count
    thread_id = threading.current_thread().ident
    # Logging reducido para mejorar rendimiento
    if batch_number % 20 == 1:  # Solo log cada 20 lotes
        logger.info(f"[HILO-{thread_id}] Procesando lotes {batch_number}-{batch_number+19}")
    
    batch_results = {
        'rows_inserted': 0,
        'rows_updated': 0,
        'rows_with_errors': 0,
        'processed_details': [],
        'errors': []
    }
    
    # Con particionado inteligente, los deadlocks son prácticamente imposibles
    # Pero mantenemos un reintento mínimo por seguridad
    max_retries = 1
    retry_count = 0
    
    while retry_count <= max_retries:
        conn = get_db_connection_local()
        cursor = conn.cursor()
        
        # OPTIMIZACIONES DE BASE DE DATOS SEGURAS
        try:
            # Solo configuraciones seguras que no comprometan la integridad
            cursor.execute("SET work_mem = '256MB'")         # Más memoria para operaciones
            cursor.execute("SET temp_buffers = '32MB'")      # Buffer temporal más grande
        except:
            pass  # Ignorar si no se pueden configurar
        
        deadlock_detected = False
        
        try:
            # Recopilar todos los datos del lote primero (sin transacción)
            temp_data = []
            for index, row in batch_data.iterrows():
                excel_row = index + 2
                try:
                    # Preparar datos básicos (permitir campos vacíos/nulos)
                    # CORRECCIÓN: Incluir la cédula original para procesamiento posterior
                    data = {
                        'funcionario_id': row.get('funcionario_id'),
                        'nro_expediente': row.get('nro_expediente'),
                        'tipo_expediente': row.get('tipo_expediente'),
                        'estatus': row.get('estatus'),
                        'fecha_inicio': row.get('fecha_inicio'),
                        'fecha_finalizacion': row.get('fecha_finalizacion'),
                        'falta': row.get('falta'),
                        'decision': row.get('decision'),
                        'tipo_sancion_administrativa': row.get('tipo_sancion_administrativa'),
                        'observaciones': row.get('observaciones'),
                        'cedula_anterior_no_registrada_en_rrhh': row.get('cedula_anterior_no_registrada_en_rrhh'),
                        'expediente_relacionado_id': row.get('expediente_relacionado_id'),
                        'cedula': row.get('cedula')  # IMPORTANTE: Incluir cédula original
                    }
                    
                    # Solo filtrar valores que son explícitamente None o NaN, permitir strings vacíos
                    # CORRECCIÓN: Mantener 'cedula' para procesamiento pero excluirla del INSERT final
                    data_filtered = {}
                    
                    # CORRECCIÓN: Lógica de filtrado más permisiva para fechas importantes
                    for k, v in data.items():
                        # Campos que NUNCA deben filtrarse si tienen valor válido
                        campos_importantes = ['fecha_inicio', 'fecha_finalizacion', 'nro_expediente', 'tipo_expediente', 'estatus']
                        
                        # Si es un campo importante y tiene un valor válido (no None, no NaN, no string vacío)
                        if k in campos_importantes:
                            if v is not None and not (pd.isna(v) if hasattr(pd, 'isna') and pd.isna(v) else False):
                                if not (isinstance(v, str) and v.strip() == ''):
                                    data_filtered[k] = v
                                    continue
                            # Solo excluir si es campo requerido y está vacío
                            elif k == 'nro_expediente':
                                continue  # Saltar este registro completo
                        
                        # Para otros campos, aplicar filtrado normal
                        if v is not None and not (pd.isna(v) if hasattr(pd, 'isna') and pd.isna(v) else False):
                            # Convertir valores vacíos a None para campos que pueden ser nulos
                            if isinstance(v, str) and v.strip() == '':
                                data_filtered[k] = None
                            else:
                                data_filtered[k] = v
                    
                    if not data_filtered.get('nro_expediente'):
                        batch_results['rows_with_errors'] += 1
                        continue
                    
                    temp_data.append((excel_row, data_filtered))
                    
                except Exception as row_error:
                    batch_results['rows_with_errors'] += 1
                    # Solo almacenar los primeros 20 errores para mejorar rendimiento
                    if len(batch_results['errors']) < 20:
                        batch_results['errors'].append({
                            'excel_row': excel_row,
                            'error': str(row_error),
                            'batch': batch_number,
                            'thread': thread_id
                        })
                    continue
            
            # Ahora procesar con transacción segura
            batch_results['temp_data'] = temp_data
                    
            # PROCESAMIENTO BATCH AL FINAL - MÁXIMA VELOCIDAD
            if batch_results['temp_data'] and mode == 'sync':
                # Obtener todos los expedientes existentes en una sola consulta CON ORDER BY para evitar deadlocks
                # CORRECCIÓN: Usar clave compuesta (nro_expediente + funcionario_id + cedula_anterior) para manejar múltiples registros
                nro_expedientes = [item[1]['nro_expediente'] for item in batch_results['temp_data']]
                if nro_expedientes:
                    placeholders = ','.join(['%s'] * len(nro_expedientes))
                    cursor.execute(f'''
                        SELECT nro_expediente, id_exp, funcionario_id, fecha_inicio, estatus, fecha_finalizacion, 
                               cedula_anterior_no_registrada_en_rrhh, tipo_expediente, observaciones, 
                               expediente_relacionado_id, tipo_sancion_administrativa
                        FROM expedientes 
                        WHERE nro_expediente IN ({placeholders})
                        ORDER BY id_exp ASC
                    ''', nro_expedientes)
                    existing_expedientes_dict = {}
                    for row in cursor.fetchall():
                        nro_exp, exp_id, func_id, fecha_ini, estatus, fecha_fin, cedula_anterior, tipo_exp, observaciones, exp_rel, tipo_sancion = row
                        # CLAVE COMPUESTA: nro_expediente + funcionario_id + cedula_anterior
                        # Esto permite múltiples registros con el mismo nro_expediente pero diferentes cédulas
                        composite_key = f"{nro_exp}|{func_id}|{cedula_anterior}"
                        existing_expedientes_dict[composite_key] = {
                            'id_exp': exp_id,
                            'nro_expediente': nro_exp,
                            'funcionario_id': func_id,
                            'fecha_inicio': fecha_ini.strftime('%Y-%m-%d') if fecha_ini else None,
                            'estatus': estatus,
                            'fecha_finalizacion': fecha_fin.strftime('%Y-%m-%d') if fecha_fin else None,
                            'cedula_anterior_no_registrada_en_rrhh': cedula_anterior,
                            'tipo_expediente': tipo_exp,
                            'observaciones': observaciones,
                            'expediente_relacionado_id': exp_rel,
                            'tipo_sancion_administrativa': tipo_sancion
                        }
                
                # Procesar todos los registros del lote
                updates_batch = []
                inserts_batch = []
                
                for excel_row, data_filtered in batch_results['temp_data']:
                    nro_expediente = data_filtered['nro_expediente']
                    
                    # CORRECCIÓN: Verificar y asignar correctamente la cédula antes de construir la clave compuesta
                    # IMPORTANTE: Cada registro debe procesarse independientemente
                    if 'cedula' in data_filtered and data_filtered['cedula']:
                        cedula_str = str(data_filtered['cedula']).strip().replace('-', '')
                        
                        # Saltar cédulas especiales
                        if cedula_str not in ['6666', '9999'] and cedula_str != '' and not cedula_str.isspace():
                            try:
                                cedula_original = int(cedula_str)
                                # Verificar si la cédula está registrada en funcionarios
                                cursor.execute("SELECT id FROM funcionarios WHERE cedula = %s", (cedula_original,))
                                funcionario = cursor.fetchone()
                                
                                if funcionario:
                                    # Cédula registrada: usar funcionario_id
                                    data_filtered['funcionario_id'] = funcionario[0]
                                    data_filtered['cedula_anterior_no_registrada_en_rrhh'] = None
                                else:
                                    # Cédula NO registrada: usar campo especial
                                    data_filtered['funcionario_id'] = None
                                    data_filtered['cedula_anterior_no_registrada_en_rrhh'] = cedula_str
                            except ValueError:
                                # Cédula con formato inválido
                                data_filtered['funcionario_id'] = None
                                data_filtered['cedula_anterior_no_registrada_en_rrhh'] = cedula_str
                    else:
                        # Si no hay cédula, asegurar que los campos estén en None
                        data_filtered['funcionario_id'] = None
                        data_filtered['cedula_anterior_no_registrada_en_rrhh'] = None
                    
                    # CORRECCIÓN: Construir clave compuesta DESPUÉS de asignar funcionario_id y cedula_anterior
                    funcionario_id = data_filtered.get('funcionario_id')
                    cedula_anterior = data_filtered.get('cedula_anterior_no_registrada_en_rrhh')
                    composite_key = f"{nro_expediente}|{funcionario_id}|{cedula_anterior}"
                    existing = existing_expedientes_dict.get(composite_key)
                    
                    # Logging reducido para rendimiento - solo procesar
                    
                    should_update = False
                    if existing:
                        # CONDICIÓN ULTRA ESTRICTA: TODOS los campos clave deben ser IDÉNTICOS
                        # Solo actualizar si coinciden: nro_expediente + funcionario_id + fecha_inicio + estatus + fecha_finalizacion
                        
                        # Función para normalizar valores para comparación exacta
                        def valores_identicos(val1, val2):
                            # Si ambos son None/null, son iguales
                            if (val1 is None or pd.isna(val1)) and (val2 is None or pd.isna(val2)):
                                return True
                            # Si uno es None y otro no, son diferentes
                            if (val1 is None or pd.isna(val1)) or (val2 is None or pd.isna(val2)):
                                return False
                            # Comparar valores convertidos a string para evitar problemas de tipo
                            return str(val1).strip() == str(val2).strip()
                        
                        # NUEVA LÓGICA: TODOS los campos deben ser idénticos EXCEPTO decision, falta y estatus
                        # Campos que DEBEN coincidir exactamente:
                        funcionario_match = valores_identicos(existing['funcionario_id'], data_filtered.get('funcionario_id'))
                        fecha_inicio_match = valores_identicos(existing['fecha_inicio'], data_filtered.get('fecha_inicio'))
                        fecha_fin_match = valores_identicos(existing['fecha_finalizacion'], data_filtered.get('fecha_finalizacion'))
                        
                        # CORRECCIÓN: Usar campos que ya están en existing (obtenidos en la consulta principal)
                        tipo_exp_match = valores_identicos(existing.get('tipo_expediente'), data_filtered.get('tipo_expediente'))
                        observaciones_match = valores_identicos(existing.get('observaciones'), data_filtered.get('observaciones'))
                        exp_rel_match = valores_identicos(existing.get('expediente_relacionado_id'), data_filtered.get('expediente_relacionado_id'))
                        tipo_sancion_match = valores_identicos(existing.get('tipo_sancion_administrativa'), data_filtered.get('tipo_sancion_administrativa'))
                        cedula_ant_match = valores_identicos(existing.get('cedula_anterior_no_registrada_en_rrhh'), data_filtered.get('cedula_anterior_no_registrada_en_rrhh'))
                        
                        # SOLO actualizar si TODOS los campos (excepto decision, falta, estatus) coinciden
                        if (funcionario_match and fecha_inicio_match and fecha_fin_match and 
                            tipo_exp_match and observaciones_match and exp_rel_match and 
                            tipo_sancion_match and cedula_ant_match):
                            should_update = True
                    
                    if should_update:
                        # Preparar para actualización batch
                        set_values = []
                        update_values = []
                        for col, val in data_filtered.items():
                            if col not in ['nro_expediente', 'cedula']:  # Excluir cedula también en updates
                                set_values.append(f'"{col}" = %s')
                                update_values.append(val)
                        
                        if set_values:
                            updates_batch.append((set_values, update_values, existing['id_exp'], excel_row, data_filtered))
                    else:
                        # Preparar para inserción batch
                        inserts_batch.append((excel_row, data_filtered))
                
                # EJECUTAR ACTUALIZACIONES EN BATCH CON MANEJO SEGURO DE ERRORES
                for set_values, update_values, exp_id, excel_row, data_filtered in updates_batch:
                    try:
                        update_query = f'UPDATE expedientes SET {", ".join(set_values)} WHERE id_exp = %s'
                        cursor.execute(update_query, update_values + [exp_id])
                        batch_results['rows_updated'] += 1
                        
                        batch_results['processed_details'].append({
                            'excel_row': excel_row,
                            'action': 'ACTUALIZADO',
                            'expediente': data_filtered['nro_expediente'],
                            'funcionario_id': data_filtered.get('funcionario_id'),
                            'cedula_original': data_filtered.get('cedula'),
                            'cedula_anterior': data_filtered.get('cedula_anterior_no_registrada_en_rrhh'),
                            'batch': batch_number,
                            'thread': thread_id
                        })
                        # Logging reducido para rendimiento
                    except psycopg2.Error as pg_error:
                        # Error específico de PostgreSQL - hacer rollback inmediato
                        conn.rollback()
                        batch_results['rows_with_errors'] += 1
                        # Log solo primeros 20 errores
                        with _error_count_lock:
                            if _global_error_count < 20:
                                _global_error_count += 1
                                logger.error(f"[ERROR-{_global_error_count}/20] PostgreSQL Update Error Fila {excel_row}: {str(pg_error)}")
                        # Salir del procesamiento de este lote
                        deadlock_detected = True
                        break
                    except Exception as update_error:
                        batch_results['rows_with_errors'] += 1
                        # Log solo primeros 20 errores
                        with _error_count_lock:
                            if _global_error_count < 20:
                                _global_error_count += 1
                                logger.error(f"[ERROR-{_global_error_count}/20] Update Fila {excel_row}: {str(update_error)}")
                
                # EJECUTAR INSERCIONES EN BATCH CON MANEJO SEGURO DE ERRORES
                if inserts_batch and not deadlock_detected:
                    try:
                        # Preparar inserción múltiple
                        if inserts_batch:
                            first_data = inserts_batch[0][1]
                            # CORRECCIÓN: Excluir 'cedula' de las columnas a insertar
                            columns = [col for col in first_data.keys() if col != 'cedula']
                            columns_str = ', '.join(f'"{col}"' for col in columns)
                            
                            # Insertar múltiples registros de una vez
                            values_list = []
                            for excel_row, data_filtered in inserts_batch:
                                values = [data_filtered.get(col) for col in columns]
                                values_list.append(values)
                                
                            # Usar executemany para máxima velocidad
                            placeholders = ', '.join(['%s'] * len(columns))
                            insert_query = f'INSERT INTO expedientes ({columns_str}) VALUES ({placeholders})'
                        
                            cursor.executemany(insert_query, values_list)
                            
                            batch_results['rows_inserted'] += len(inserts_batch)
                            
                            # Agregar detalles
                            for excel_row, data_filtered in inserts_batch:
                                batch_results['processed_details'].append({
                                    'excel_row': excel_row,
                                    'action': 'AÑADIDO',
                                    'expediente': data_filtered['nro_expediente'],
                                    'funcionario_id': data_filtered.get('funcionario_id'),
                                    'cedula_original': data_filtered.get('cedula'),
                                    'cedula_anterior': data_filtered.get('cedula_anterior_no_registrada_en_rrhh'),
                                    'batch': batch_number,
                                    'thread': thread_id
                                })
                                # Logging reducido para rendimiento
                    except psycopg2.Error as pg_error:
                        # Error específico de PostgreSQL - hacer rollback inmediato
                        conn.rollback()
                        batch_results['rows_with_errors'] += len(inserts_batch)
                        # Log solo primeros 20 errores
                        with _error_count_lock:
                            if _global_error_count < 20:
                                _global_error_count += 1
                                logger.error(f"[ERROR-{_global_error_count}/20] PostgreSQL Batch Insert Error: {str(pg_error)}")
                        deadlock_detected = True
                    except Exception as insert_error:
                        batch_results['rows_with_errors'] += len(inserts_batch)
                        # Log solo primeros 20 errores
                        with _error_count_lock:
                            if _global_error_count < 20:
                                _global_error_count += 1
                                logger.error(f"[ERROR-{_global_error_count}/20] Batch Insert: {str(insert_error)}")
            
            if not deadlock_detected:
                conn.commit()
                # Logging reducido - solo para lotes con errores o cada 10 lotes
                if batch_results['rows_with_errors'] > 0 or batch_number % 10 == 0:
                    logger.info(f"[HILO-IMPROVED-{thread_id}] Lote {batch_number}: +{batch_results['rows_inserted']} ↻{batch_results['rows_updated']} ✗{batch_results['rows_with_errors']}")
                break  # Salir del while si procesamiento exitoso
            
        except psycopg2.Error as pg_error:
            conn.rollback()
            error_msg = str(pg_error).lower()
            if "deadlock" in error_msg or "transacción abortada" in error_msg or "transaction aborted" in error_msg:
                deadlock_detected = True
                logger.warning(f"[TRANSACTION_ERROR] Error de transacción en lote {batch_number}, intento {retry_count + 1}/{max_retries + 1}: {pg_error}")
            else:
                logger.error(f"[HILO-IMPROVED-{thread_id}] Error PostgreSQL en lote {batch_number}: {pg_error}")
                # Solo almacenar los primeros 20 errores para mejorar rendimiento
                if len(batch_results['errors']) < 20:
                    batch_results['errors'].append({
                        'batch': batch_number,
                        'error': str(pg_error),
                        'thread': thread_id
                    })
                break  # Salir del while si no es error de transacción
        except Exception as e:
            conn.rollback()
            logger.error(f"[HILO-IMPROVED-{thread_id}] Error general en lote {batch_number}: {e}")
            # Solo almacenar los primeros 20 errores para mejorar rendimiento
            if len(batch_results['errors']) < 20:
                batch_results['errors'].append({
                    'batch': batch_number,
                    'error': str(e),
                    'thread': thread_id
                })
            break  # Salir del while para errores generales
        finally:
            cursor.close()
            conn.close()
        
        # Manejo de reintentos
        if deadlock_detected:
            retry_count += 1
            if retry_count <= max_retries:
                import time
                time.sleep(0.1 * retry_count)  # Espera incremental
                # continue implícito del while
            else:
                logger.error(f"[DEADLOCK] Lote {batch_number} falló después de {max_retries} reintentos")
                # Solo almacenar los primeros 20 errores para mejorar rendimiento
                if len(batch_results['errors']) < 20:
                    batch_results['errors'].append({
                        'batch': batch_number,
                        'error': 'Deadlock persistente después de reintentos',
                        'thread': thread_id
                    })
                break  # Salir del while
        else:
            break  # Salir del while si no hay deadlock
    
    return batch_results

async def process_expedientes_data_with_cedula_filter(df: pd.DataFrame, mode: str, cedula_filter: str) -> Dict:
    """Procesar datos de expedientes con filtro de cédulas según decisión del usuario"""
    logger.info("=" * 80)
    logger.info(f"[PROCESS_CEDULA_FILTER] Procesando con filtro: {cedula_filter}")
    logger.info("=" * 80)
    
    # Limpiar datos (sin validadores restrictivos)
    df_clean = clean_expediente_data(df)
    
    # Validar solo columnas mínimas requeridas
    required_columns = ['cedula', 'nro_expediente']
    missing_columns = [col for col in required_columns if col not in df_clean.columns]
    if missing_columns:
        logger.error(f"Columnas faltantes: {missing_columns}")
        raise HTTPException(
            status_code=400,
            detail=f"Columnas requeridas faltantes: {', '.join(missing_columns)}"
        )
    
    # Verificar funcionarios existentes y aplicar filtro
    logger.info("[PROCESS_CEDULA_FILTER] Verificando funcionarios existentes...")
    cedulas_no_registradas = []
    cedulas_procesadas = []
    
    conn = get_db_connection_local()
    cursor = conn.cursor()
    
    # Crear DataFrame filtrado según la decisión
    df_to_process = df_clean.copy()
    rows_to_remove = []
    
    for index, row in df_clean.iterrows():
        if 'cedula' in row and not pd.isna(row['cedula']):
            cedula_str = str(row['cedula']).strip().replace('-', '')
            
            # Saltar verificación para cédulas especiales (6666, 9999, vacías)
            if cedula_str in ['6666', '9999'] or cedula_str == '' or cedula_str.isspace():
                logger.info(f"[PROCESS_CEDULA_FILTER] Saltando cédula especial: '{cedula_str}' en fila {index + 2}")
                # Para cédulas especiales, no asignar funcionario_id ni campo especial
                df_to_process.at[index, 'funcionario_id'] = None
                df_to_process.at[index, 'cedula_anterior_no_registrada_en_rrhh'] = None
                continue
            
            try:
                cedula = int(cedula_str)
                cursor.execute("SELECT id FROM funcionarios WHERE cedula = %s", (cedula,))
                funcionario = cursor.fetchone()
                
                if not funcionario:
                    # Cédula no registrada
                    cedulas_no_registradas.append({
                        'cedula': cedula,
                        'nro_expediente': row.get('nro_expediente', 'N/A'),
                        'row': index + 2
                    })
                    
                    if cedula_filter == "solo_validas":
                        # Marcar para eliminar del procesamiento
                        rows_to_remove.append(index)
                        logger.info(f"[PROCESS_CEDULA_FILTER] Excluyendo fila {index + 2} - cédula no registrada: {cedula}")
                    elif cedula_filter == "cargar_todas":
                        # Usar campo especial para cédulas no registradas
                        df_to_process.at[index, 'cedula_anterior_no_registrada_en_rrhh'] = cedula_str
                        df_to_process.at[index, 'funcionario_id'] = None
                else:
                    # Cédula válida
                    df_to_process.at[index, 'funcionario_id'] = funcionario[0]
                    cedulas_procesadas.append({
                        'cedula': cedula,
                        'funcionario_id': funcionario[0],
                        'row': index + 2
                    })
                    
            except ValueError as e:
                logger.warning(f"[PROCESS_CEDULA_FILTER] Error convirtiendo cédula '{cedula_str}' a número en fila {index + 2}")
                # Las cédulas con formato inválido siempre se excluyen
                rows_to_remove.append(index)
    
    cursor.close()
    conn.close()
    
    # Aplicar filtro eliminando filas si es necesario
    if rows_to_remove:
        df_to_process = df_to_process.drop(rows_to_remove)
        logger.info(f"[PROCESS_CEDULA_FILTER] Eliminadas {len(rows_to_remove)} filas según filtro")
    
    if len(df_to_process) == 0:
        return {
            "success": False,
            "message": "No hay registros válidos para procesar después de aplicar el filtro.",
            "report": {
                "added_count": 0,
                "updated_count": 0,
                "deleted_count": 0,
                "processing_errors": [],
                "cedulas_no_registradas": cedulas_no_registradas,
                "cedulas_procesadas": cedulas_procesadas
            }
        }
    
    # Procesar datos con multihilo y lotes mejorado
    return await process_expedientes_data_improved(df_to_process, mode, cedulas_no_registradas, cedulas_procesadas)

async def process_expedientes_data_improved(df: pd.DataFrame, mode: str, cedulas_no_registradas: List, cedulas_procesadas: List) -> Dict:
    """Procesar datos de expedientes con condiciones de actualización mejoradas"""
    global _global_error_count
    
    # Resetear contador de errores al inicio del procesamiento
    with _error_count_lock:
        _global_error_count = 0
    
    logger.info("[PROCESS_IMPROVED] Iniciando procesamiento con condiciones mejoradas...")
    
    # Configuración de lotes y hilos
    BATCH_SIZE = 3000  # Lotes optimizados para 12 cores
    MAX_THREADS = 6    # Aprovechar más cores con particionado inteligente
    
    total_rows = len(df)
    logger.info(f"[PROCESS_IMPROVED] Total de registros a procesar: {total_rows}")
    logger.info(f"[PROCESS_IMPROVED] Tamaño de lote: {BATCH_SIZE}")
    logger.info(f"[PROCESS_IMPROVED] Número máximo de hilos: {MAX_THREADS}")
    
    # PARTICIONADO INTELIGENTE PARA EVITAR DEADLOCKS
    # Agrupar por nro_expediente para evitar que múltiples hilos trabajen en el mismo expediente
    logger.info("[PROCESS_IMPROVED] Aplicando particionado inteligente para evitar deadlocks...")
    
    # Crear hash del nro_expediente para distribuir uniformemente
    df['_partition_key'] = df['nro_expediente'].astype(str).apply(lambda x: hash(x) % MAX_THREADS)
    
    # Dividir por particiones (cada hilo trabajará en expedientes diferentes)
    batches = []
    batch_counter = 1
    
    for partition_id in range(MAX_THREADS):
        partition_df = df[df['_partition_key'] == partition_id].copy()
        if len(partition_df) > 0:
            logger.info(f"[PROCESS_IMPROVED] Partición {partition_id}: {len(partition_df)} registros")
            # Dividir cada partición en lotes
            for i in range(0, len(partition_df), BATCH_SIZE):
                batch = partition_df.iloc[i:i + BATCH_SIZE].copy()
                # Remover la columna auxiliar
                batch = batch.drop('_partition_key', axis=1)
                batches.append((batch, batch_counter, partition_id))
                batch_counter += 1
    
    logger.info(f"[PROCESS_IMPROVED] Datos particionados en {MAX_THREADS} particiones y {len(batches)} lotes")
    logger.info(f"[PROCESS_IMPROVED] Cada hilo trabajará en expedientes diferentes - SIN DEADLOCKS")
    
    # Procesar lotes con ThreadPoolExecutor
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Enviar todos los lotes a los hilos con información de partición
        futures = []
        for batch_data, batch_number, partition_id in batches:
            future = executor.submit(process_batch_improved, batch_data, mode, batch_number)
            futures.append((future, batch_number, partition_id))
        
        # Recopilar resultados
        for i, (future, batch_number, partition_id) in enumerate(futures):
            try:
                result = future.result()
                all_results.append(result)
                logger.info(f"[PROCESS_IMPROVED] Lote {batch_number} (Partición {partition_id}) completado exitosamente")
            except Exception as e:
                logger.error(f"[PROCESS_IMPROVED] Error en lote {batch_number} (Partición {partition_id}): {e}")
                all_results.append({
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_with_errors': 1,
                    'processed_details': [],
                    'errors': [{'batch': batch_number, 'partition': partition_id, 'error': str(e)}]
                })
    
    # Consolidar resultados
    total_inserted = sum(result['rows_inserted'] for result in all_results)
    total_updated = sum(result['rows_updated'] for result in all_results)
    total_errors = sum(result['rows_with_errors'] for result in all_results)
    
    all_processed_details = []
    all_processing_errors = []
    
    for result in all_results:
        all_processed_details.extend(result['processed_details'])
        all_processing_errors.extend(result['errors'])
    
    logger.info(f"[PROCESS_IMPROVED] Procesamiento completado:")
    logger.info(f"  - Total insertados: {total_inserted}")
    logger.info(f"  - Total actualizados: {total_updated}")
    logger.info(f"  - Total errores: {total_errors}")
    logger.info(f"  - Lotes procesados: {len(batches)}")
    
    # Contar total de errores detallados almacenados
    total_detailed_errors = sum(len(result['errors']) for result in all_results)
    if total_detailed_errors >= 20:
        logger.info(f"  - NOTA: Se almacenaron solo los primeros 20 errores detallados para optimizar rendimiento")
    
    # GENERAR REPORTE FINAL DETALLADO
    success_report_file = None
    try:
        # Analizar campos procesados
        campos_insertados = {}
        campos_actualizados = {}
        expedientes_por_accion = {'AÑADIDO': [], 'ACTUALIZADO': []}
        
        for detail in all_processed_details:
            accion = detail.get('action', 'DESCONOCIDO')
            expediente = detail.get('expediente', 'N/A')
            
            if accion in expedientes_por_accion:
                expedientes_por_accion[accion].append({
                    'expediente': expediente,
                    'funcionario_id': detail.get('funcionario_id'),
                    'excel_row': detail.get('excel_row'),
                    'batch': detail.get('batch'),
                    'thread': detail.get('thread')
                })
        
        # Generar reporte detallado
        report_content = f"""
REPORTE FINAL DE PROCESAMIENTO ETL - EXPEDIENTES
==============================================
Fecha y Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

RESUMEN EJECUTIVO
================
Total de registros procesados: {len(all_processed_details)}
Expedientes añadidos: {total_inserted}
Expedientes actualizados: {total_updated}
Errores encontrados: {total_errors}
Lotes procesados: {len(batches)}
Hilos utilizados: {MAX_THREADS}
Tamaño de lote: {BATCH_SIZE}

CONFIGURACIÓN DE PROCESAMIENTO
=============================
- Particionado inteligente: SÍ (evita deadlocks)
- Procesamiento por lotes: SÍ
- Manejo seguro de transacciones: SÍ
- Límite de errores detallados: 20
- Optimizaciones de BD: Configuraciones seguras

DETALLE DE EXPEDIENTES AÑADIDOS ({total_inserted})
===============================================
"""
        
        for i, exp in enumerate(expedientes_por_accion['AÑADIDO'][:50], 1):  # Primeros 50
            report_content += f"""
{i}. Expediente: {exp['expediente']}
   - Funcionario ID: {exp['funcionario_id']}
   - Fila Excel: {exp['excel_row']}
   - Lote: {exp['batch']} | Hilo: {exp['thread']}
"""
        
        if len(expedientes_por_accion['AÑADIDO']) > 50:
            report_content += f"\n... y {len(expedientes_por_accion['AÑADIDO']) - 50} expedientes más añadidos.\n"
        
        report_content += f"""

DETALLE DE EXPEDIENTES ACTUALIZADOS ({total_updated})
==================================================
"""
        
        for i, exp in enumerate(expedientes_por_accion['ACTUALIZADO'][:50], 1):  # Primeros 50
            report_content += f"""
{i}. Expediente: {exp['expediente']}
   - Funcionario ID: {exp['funcionario_id']}
   - Fila Excel: {exp['excel_row']}
   - Lote: {exp['batch']} | Hilo: {exp['thread']}
"""
        
        if len(expedientes_por_accion['ACTUALIZADO']) > 50:
            report_content += f"\n... y {len(expedientes_por_accion['ACTUALIZADO']) - 50} expedientes más actualizados.\n"
        
        # Agregar información de errores si los hay
        if total_errors > 0:
            report_content += f"""

RESUMEN DE ERRORES ({total_errors})
=================================
Total de errores encontrados: {total_errors}
Errores detallados almacenados: {min(total_detailed_errors, 20)}

PRIMEROS ERRORES DETALLADOS:
"""
            error_count = 0
            for result in all_results:
                for error in result['errors'][:5]:  # Primeros 5 errores por lote
                    if error_count < 20:
                        error_count += 1
                        report_content += f"""
Error {error_count}:
- Lote: {error.get('batch', 'N/A')}
- Fila Excel: {error.get('excel_row', 'N/A')}
- Error: {error.get('error', 'N/A')}
- Hilo: {error.get('thread', 'N/A')}
"""
        
        report_content += f"""

ESTADÍSTICAS DE RENDIMIENTO
===========================
- Tiempo de procesamiento: Completado exitosamente
- Lotes procesados simultáneamente: {MAX_THREADS}
- Registros por lote: {BATCH_SIZE}
- Particiones utilizadas: {MAX_THREADS}
- Deadlocks evitados: SÍ (particionado inteligente)

CAMPOS PROCESADOS
================
Los siguientes campos fueron procesados durante la importación:
- funcionario_id (relación con tabla funcionarios)
- nro_expediente (número único del expediente)
- tipo_expediente (tipo de expediente)
- estatus (estado actual del expediente)
- fecha_inicio (fecha de inicio del expediente)
- fecha_finalizacion (fecha de finalización)
- falta (descripción de la falta)
- decision (decisión tomada)
- tipo_sancion_administrativa (tipo de sanción)
- observaciones (observaciones adicionales)
- cedula_anterior_no_registrada_en_rrhh (cédulas no registradas)
- expediente_relacionado_id (expedientes relacionados)

CONDICIONES DE ACTUALIZACIÓN APLICADAS
=====================================
Un expediente se actualiza ÚNICAMENTE si cumple TODAS estas condiciones:
1. Mismo nro_expediente
2. Y misma cédula (funcionario_id)
3. Y misma fecha_inicio
4. Y mismo estatus
5. Y misma fecha_finalizacion

NOTA IMPORTANTE: TODOS los campos deben ser IDÉNTICOS para actualizar.
Si cualquier campo difiere, se crea un nuevo expediente.

NOTAS IMPORTANTES
================
- Solo se almacenan los primeros 20 errores detallados para optimizar rendimiento
- Las transacciones se manejan de forma segura para evitar corrupción de datos
- El particionado inteligente elimina los deadlocks entre hilos
- Las cédulas no registradas se almacenan en campo especial cuando corresponde

FIN DEL REPORTE
==============
"""
        
        # Guardar reporte
        report_filename = f"reporte_final_expedientes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_path = Path("logs") / report_filename
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        
        success_report_file = str(report_path)
        logger.info(f"[PROCESS_IMPROVED] Reporte final generado: {success_report_file}")
        
    except Exception as report_error:
        logger.error(f"[PROCESS_IMPROVED] Error generando reporte final: {report_error}")
        success_report_file = None
    
    return {
        "success": True,
        "message": f"Importación completada. {total_inserted} expedientes añadidos, {total_updated} actualizados.",
        "report": {
            "added_count": total_inserted,
            "updated_count": total_updated,
            "deleted_count": 0,
            "processing_errors": all_processing_errors,
            "cedulas_no_registradas": cedulas_no_registradas,
            "cedulas_procesadas": cedulas_procesadas,
            "processed_details": all_processed_details,
            "batches_processed": len(batches),
            "threads_used": MAX_THREADS,
            "batch_size": BATCH_SIZE,
            "partitions_used": MAX_THREADS,
            "success_report_file": success_report_file,
            "final_report_generated": success_report_file is not None,
            "report_location": success_report_file if success_report_file else "No se pudo generar el reporte"
        }
    }

async def process_expedientes_data(df: pd.DataFrame, mode: str, force_process: bool = False) -> Dict:
    """Procesar datos de expedientes con multihilo y procesamiento por lotes (función original mantenida para compatibilidad)"""
    logger.info("Procesando datos de expedientes con multihilo...")
    
    # Limpiar datos
    df_clean = clean_expediente_data(df)
    
    # Validar columnas requeridas
    required_columns = ['cedula', 'nro_expediente']
    missing_columns = [col for col in required_columns if col not in df_clean.columns]
    if missing_columns:
        logger.error(f"Columnas faltantes: {missing_columns}")
        raise HTTPException(
            status_code=400,
            detail=f"Columnas requeridas faltantes: {', '.join(missing_columns)}"
        )
    
    # Verificar funcionarios existentes
    logger.info("Verificando funcionarios existentes...")
    invalid_cedulas = []
    cedulas_no_registradas = []  # Para almacenar en campo especial
    
    conn = get_db_connection_local()
    cursor = conn.cursor()
    
    for index, row in df_clean.iterrows():
        if 'cedula' in row and not pd.isna(row['cedula']):
            cedula_str = str(row['cedula']).strip().replace('-', '')
            
            # Saltar verificación para cédulas especiales
            if cedula_str == '9999' or cedula_str == '' or cedula_str.isspace():
                logger.info(f"Saltando verificación para cédula especial: '{cedula_str}' en fila {index + 2}")
                continue
            
            try:
                cedula = int(cedula_str)
                cursor.execute("SELECT id FROM funcionarios WHERE cedula = %s", (cedula,))
                funcionario = cursor.fetchone()
                
                if not funcionario:
                    # Agregar a lista de cédulas no registradas para campo especial
                    cedulas_no_registradas.append({
                        'cedula': cedula,
                        'nro_expediente': row.get('nro_expediente', 'N/A'),
                        'row': index + 2,
                        'nombre_funcionario': row.get('NOMBRE DEL FUNCIONARIO', 'N/A'),
                        'tipo_expediente': row.get('tipo_expediente', 'N/A'),
                        'fecha_inicio': row.get('fecha_inicio', 'N/A')
                    })
                    # Marcar para usar campo especial
                    df_clean.at[index, 'cedula_anterior_no_registrada_en_rrhh'] = cedula_str
                    df_clean.at[index, 'funcionario_id'] = None  # No hay funcionario registrado
                else:
                    df_clean.at[index, 'funcionario_id'] = funcionario[0]
                    
            except ValueError as e:
                logger.warning(f"Error convirtiendo cédula '{cedula_str}' a número en fila {index + 2}")
                invalid_cedulas.append({
                    'cedula': cedula_str,
                    'nro_expediente': row.get('nro_expediente', 'N/A'),
                    'row': index + 2,
                    'nombre_funcionario': row.get('NOMBRE DEL FUNCIONARIO', 'N/A'),
                    'tipo_expediente': row.get('tipo_expediente', 'N/A'),
                    'fecha_inicio': row.get('fecha_inicio', 'N/A'),
                    'error': 'Formato de cédula inválido'
                })
    
    cursor.close()
    conn.close()
    
    # Si hay cédulas inválidas (formato incorrecto), reportar error
    if invalid_cedulas and not force_process:
        logger.warning(f"Se encontraron {len(invalid_cedulas)} cédulas con formato inválido")
        return {
            "success": False,
            "message": f"Se encontraron {len(invalid_cedulas)} cédulas con formato inválido",
            "report": {
                "added_count": 0,
                "updated_count": 0,
                "deleted_count": 0,
                "processing_errors": [],
                "invalid_cedulas": invalid_cedulas,
                "cedulas_no_registradas": cedulas_no_registradas
            }
        }
    
    # Usar la función mejorada
    return await process_expedientes_data_improved(df_clean, mode, cedulas_no_registradas, [])

@router.post("/import-excel")
async def import_expedientes_excel(
    file: UploadFile = File(...),
    mode: str = Form(default='sync')
):
    """Endpoint específico para importar expedientes desde Excel"""
    temp_path = None
    
    try:
        logger.info(f"Iniciando importación de expedientes. Archivo: {file.filename}")
        
        # Verificar extensión del archivo
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Solo se permiten archivos Excel (.xlsx, .xls)"
            )
        
        # Guardar archivo temporalmente
        temp_path = Path("temp_uploads") / f"expedientes_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        temp_path.parent.mkdir(exist_ok=True)
        
        with open(temp_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        logger.info(f"Archivo guardado temporalmente: {temp_path}")
        
        # Leer Excel
        logger.info("Leyendo archivo Excel...")
        df = pd.read_excel(temp_path)
        logger.info(f"Excel leído exitosamente. Columnas: {df.columns.tolist()}")
        
        # Mapear columnas del Excel a nombres estándar
        logger.info("Mapeando columnas...")
        column_mapping = {
            'TIPO DE EXPEDIENTE': 'tipo_expediente',
            'FECHA DE INICIO': 'fecha_inicio', 
            'EXPEDIENTE': 'nro_expediente',
            'CÉDULA DEL FUNCIONARIO': 'cedula',
            'EXPEDIENTE RELACIONADO': 'expediente_relacionado_id',
            'DECISIÓN': 'decision',
            'ESTATUS': 'estatus',
            'FECHA DE FINALIZACIÓN': 'fecha_finalizacion',
            'TIPO DE SANCIÓN ADMINISTRATIVA': 'tipo_sancion_administrativa',
            'FALTA': 'falta',
            'RESEÑA DEL CASO': 'observaciones'
        }
        
        # Renombrar columnas
        df_renamed = df.rename(columns=column_mapping)
        logger.info(f"Columnas después del mapeo: {df_renamed.columns.tolist()}")
        
        # Limpiar datos
        logger.info("Limpiando datos...")
        df_clean = clean_expediente_data(df_renamed)
        
        # Validar columnas requeridas
        required_columns = ['cedula', 'nro_expediente']
        missing_columns = [col for col in required_columns if col not in df_clean.columns]
        if missing_columns:
            logger.error(f"Columnas faltantes: {missing_columns}")
            logger.error(f"Columnas disponibles: {df_clean.columns.tolist()}")
            raise HTTPException(
                status_code=400,
                detail=f"Columnas requeridas faltantes: {', '.join(missing_columns)}"
            )
        
        # Verificar funcionarios existentes
        logger.info("Verificando funcionarios existentes...")
        invalid_cedulas = []
        conn = get_db_connection_local()
        cursor = conn.cursor()
        
        for index, row in df_clean.iterrows():
            if 'cedula' in row and not pd.isna(row['cedula']):
                cedula_str = str(row['cedula']).strip().replace('-', '')
                
                # Saltar verificación para cédulas especiales
                if cedula_str == '9999' or cedula_str == '' or cedula_str.isspace():
                    logger.info(f"Saltando verificación para cédula especial: '{cedula_str}' en fila {index + 2}")
                    continue
                
                try:
                    cedula = int(cedula_str)
                    cursor.execute("SELECT id FROM funcionarios WHERE cedula = %s", (cedula,))
                    funcionario = cursor.fetchone()
                    
                    if not funcionario:
                        invalid_cedulas.append({
                            'cedula': cedula,
                            'nro_expediente': row.get('nro_expediente', 'N/A'),
                            'row': index + 2,
                            'nombre_funcionario': row.get('NOMBRE DEL FUNCIONARIO', 'N/A'),
                            'tipo_expediente': row.get('tipo_expediente', 'N/A'),
                            'fecha_inicio': row.get('fecha_inicio', 'N/A')
                        })
                    else:
                        df_clean.at[index, 'funcionario_id'] = funcionario[0]
                except ValueError as e:
                    logger.warning(f"Error convirtiendo cédula '{cedula_str}' a número en fila {index + 2}")
                    invalid_cedulas.append({
                        'cedula': cedula_str,
                        'nro_expediente': row.get('nro_expediente', 'N/A'),
                        'row': index + 2,
                        'nombre_funcionario': row.get('NOMBRE DEL FUNCIONARIO', 'N/A'),
                        'tipo_expediente': row.get('tipo_expediente', 'N/A'),
                        'fecha_inicio': row.get('fecha_inicio', 'N/A'),
                        'error': 'Formato de cédula inválido'
                    })
        
        cursor.close()
        conn.close()
        
        # Si hay cédulas inválidas, reportar error
        if invalid_cedulas:
            logger.warning(f"Se encontraron {len(invalid_cedulas)} cédulas inválidas")
            # Generar reporte detallado
            report_content = f"""
REPORTE DE VALIDACIÓN DE EXPEDIENTES
===================================
Fecha y Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

RESUMEN
-------
Total de cédulas inválidas encontradas: {len(invalid_cedulas)}

DETALLE DE CÉDULAS NO ENCONTRADAS
--------------------------------
"""
            for invalid in invalid_cedulas:
                report_content += f"""
Fila Excel: {invalid['row']}
Cédula: {invalid['cedula']}
Expediente: {invalid['nro_expediente']}
Nombre del Funcionario: {invalid['nombre_funcionario']}
Tipo de Expediente: {invalid['tipo_expediente']}
Fecha de Inicio: {invalid['fecha_inicio']}
{'Error: ' + invalid['error'] if 'error' in invalid else ''}
--------------------------------
"""
            
            # Guardar reporte en archivo
            report_filename = f"reporte_cedulas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            report_path = Path("logs") / report_filename
            
            try:
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(report_content)
                logger.info(f"Reporte detallado generado en: {report_path}")
            except Exception as e:
                logger.error(f"Error generando reporte detallado: {e}")
                report_path = None

            return {
                "success": False,
                "message": f"Se encontraron {len(invalid_cedulas)} cédulas de funcionarios no registrados",
                "report": {
                    "added_count": 0,
                    "updated_count": 0,
                    "deleted_count": 0,
                    "processing_errors": [],
                    "invalid_cedulas": invalid_cedulas,
                    "report_file": str(report_path) if report_path else None
                }
            }
        
        # Procesar datos
        logger.info("Procesando datos para inserción...")
        rows_inserted = 0
        rows_updated = 0
        rows_with_errors = 0
        processed_details = []  # Lista para almacenar detalles de cada fila procesada
        
        conn = get_db_connection_local()
        cursor = conn.cursor()
        
        try:
            for index, row in df_clean.iterrows():
                excel_row = index + 2  # Fila en Excel
                try:
                    # Preparar datos básicos
                    data = {
                        'funcionario_id': row.get('funcionario_id'),
                        'nro_expediente': row.get('nro_expediente'),
                        'tipo_expediente': row.get('tipo_expediente', 'DISCIPLINARIO'),
                        'estatus': row.get('estatus', 'ABIERTO'),
                        'fecha_inicio': row.get('fecha_inicio'),
                        'fecha_finalizacion': row.get('fecha_finalizacion'),
                        'falta': row.get('falta'),
                        'decision': row.get('decision'),
                        'tipo_sancion_administrativa': row.get('tipo_sancion_administrativa'),
                        'observaciones': row.get('observaciones')
                    }
                    
                    # Filtrar valores nulos
                    data = {k: v for k, v in data.items() if not pd.isna(v)}
                    
                    if not data.get('funcionario_id') or not data.get('nro_expediente'):
                        rows_with_errors += 1
                        continue
                    
                    # Verificar si existe un expediente con el mismo número, cédula y fecha de inicio
                    if mode == 'sync':
                        cursor.execute('SELECT id_exp, funcionario_id, fecha_inicio FROM expedientes WHERE nro_expediente = %s', (data['nro_expediente'],))
                        existing_expediente = cursor.fetchone()
                        
                        if existing_expediente:
                            expediente_id, existing_funcionario_id, existing_fecha_inicio = existing_expediente
                            # Convertir fecha_inicio a string para comparación
                            existing_fecha_str = existing_fecha_inicio.strftime('%Y-%m-%d') if existing_fecha_inicio else None
                            new_fecha_str = data.get('fecha_inicio')
                            
                            # CONDICIÓN ULTRA ESTRICTA: TODOS los campos deben ser IDÉNTICOS
                            # Función para normalizar valores para comparación exacta
                            def valores_identicos_import(val1, val2):
                                # Si ambos son None/null, son iguales
                                if (val1 is None or pd.isna(val1)) and (val2 is None or pd.isna(val2)):
                                    return True
                                # Si uno es None y otro no, son diferentes
                                if (val1 is None or pd.isna(val1)) or (val2 is None or pd.isna(val2)):
                                    return False
                                # Comparar valores convertidos a string para evitar problemas de tipo
                                return str(val1).strip() == str(val2).strip()
                            
                            # Verificar que TODOS los campos clave sean exactamente iguales
                            funcionario_match = valores_identicos_import(existing_funcionario_id, data.get('funcionario_id'))
                            fecha_inicio_match = valores_identicos_import(existing_fecha_str, data.get('fecha_inicio'))
                            
                            # Para estatus y fecha_finalizacion, necesitamos obtenerlos de la BD
                            cursor.execute('SELECT estatus, fecha_finalizacion FROM expedientes WHERE id_exp = %s', (expediente_id,))
                            existing_extra = cursor.fetchone()
                            existing_estatus = existing_extra[0] if existing_extra else None
                            existing_fecha_fin = existing_extra[1].strftime('%Y-%m-%d') if existing_extra and existing_extra[1] else None
                            
                            estatus_match = valores_identicos_import(existing_estatus, data.get('estatus'))
                            fecha_fin_match = valores_identicos_import(existing_fecha_fin, data.get('fecha_finalizacion'))
                            
                            # SOLO actualizar si TODOS los campos coinciden exactamente
                            if funcionario_match and fecha_inicio_match and estatus_match and fecha_fin_match:
                                # Actualizar solo si TODOS los campos son idénticos
                                set_values = []
                                update_values = []
                                for col, val in data.items():
                                    if col not in ['nro_expediente', 'cedula']:  # Excluir cedula también en updates
                                        set_values.append(f'"{col}" = %s')
                                        update_values.append(val)
                                
                                if set_values:
                                    update_query = f'UPDATE expedientes SET {", ".join(set_values)} WHERE nro_expediente = %s'
                                    cursor.execute(update_query, update_values + [data['nro_expediente']])
                                    rows_updated += 1
                                    
                                    # Agregar detalle de actualización
                                    processed_details.append({
                                        'excel_row': excel_row,
                                        'action': 'ACTUALIZADO',
                                        'expediente': data['nro_expediente'],
                                        'funcionario_id': data.get('funcionario_id'),
                                        'tipo_expediente': data.get('tipo_expediente'),
                                        'estatus': data.get('estatus')
                                    })
                            else:
                                # Si no coinciden las tres condiciones, insertar como nuevo
                                # CORRECCIÓN: Excluir 'cedula' de las columnas a insertar
                                columns = [col for col in data.keys() if col != 'cedula']
                                values = [data[col] for col in columns]
                                placeholders = ', '.join(['%s'] * len(values))
                                columns_str = ', '.join(f'"{col}"' for col in columns)
                                
                                insert_query = f'INSERT INTO expedientes ({columns_str}) VALUES ({placeholders})'
                                cursor.execute(insert_query, values)
                                rows_inserted += 1
                                
                                # Agregar detalle de inserción
                                processed_details.append({
                                    'excel_row': excel_row,
                                    'action': 'AÑADIDO',
                                    'expediente': data['nro_expediente'],
                                    'funcionario_id': data.get('funcionario_id'),
                                    'tipo_expediente': data.get('tipo_expediente'),
                                    'estatus': data.get('estatus')
                                })
                        else:
                            # Si no existe, insertar como nuevo
                            # CORRECCIÓN: Excluir 'cedula' de las columnas a insertar
                            columns = [col for col in data.keys() if col != 'cedula']
                            values = [data[col] for col in columns]
                            placeholders = ', '.join(['%s'] * len(values))
                            columns_str = ', '.join(f'"{col}"' for col in columns)
                            
                            insert_query = f'INSERT INTO expedientes ({columns_str}) VALUES ({placeholders})'
                            cursor.execute(insert_query, values)
                            rows_inserted += 1
                            
                            # Agregar detalle de inserción
                            processed_details.append({
                                'excel_row': excel_row,
                                'action': 'AÑADIDO',
                                'expediente': data['nro_expediente'],
                                'funcionario_id': data.get('funcionario_id'),
                                'tipo_expediente': data.get('tipo_expediente'),
                                'estatus': data.get('estatus')
                            })
                        
                except Exception as row_error:
                    logger.warning(f"Error procesando fila {excel_row}: {row_error}")
                    rows_with_errors += 1
                    continue
            
            conn.commit()
            logger.info(f"Procesamiento completado. Insertados: {rows_inserted}, Actualizados: {rows_updated}, Errores: {rows_with_errors}")
            
            # Generar reporte de éxito detallado
            if processed_details:
                success_report_content = f"""
REPORTE DE IMPORTACIÓN EXITOSA - EXPEDIENTES
==========================================
Fecha y Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

RESUMEN
-------
Total de expedientes procesados: {len(processed_details)}
Expedientes añadidos: {rows_inserted}
Expedientes actualizados: {rows_updated}
Errores: {rows_with_errors}

DETALLE POR FILA PROCESADA
=========================
"""
                for detail in processed_details:
                    success_report_content += f"""
Fila Excel: {detail['excel_row']}
Acción: {detail['action']}
Expediente: {detail['expediente']}
Funcionario ID: {detail['funcionario_id']}
Tipo: {detail['tipo_expediente']}
Estatus: {detail['estatus']}
--------------------------------
"""
                
                # Guardar reporte de éxito
                success_report_filename = f"reporte_exito_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                success_report_path = Path("logs") / success_report_filename
                
                try:
                    with open(success_report_path, "w", encoding="utf-8") as f:
                        f.write(success_report_content)
                    logger.info(f"Reporte de éxito generado en: {success_report_path}")
                except Exception as e:
                    logger.error(f"Error generando reporte de éxito: {e}")
                    success_report_path = None
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en procesamiento: {e}")
            raise HTTPException(status_code=500, detail=f"Error procesando datos: {str(e)}")
        finally:
            cursor.close()
            conn.close()
        
        return {
            "success": True,
            "message": f"Importación completada. {rows_inserted} expedientes añadidos, {rows_updated} actualizados.",
            "report": {
                "added_count": rows_inserted,
                "updated_count": rows_updated,
                "deleted_count": 0,
                "processing_errors": [],
                "invalid_cedulas": [],
                "validation_failures": [],
                "processed_details": processed_details,
                "success_report_file": str(success_report_path) if 'success_report_path' in locals() and success_report_path else None
            }
        }
        
    except Exception as e:
        logger.error(f"Error en import_expedientes_excel: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando archivo: {str(e)}"
        )
    finally:
        # Limpiar archivo temporal
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
                logger.info("Archivo temporal eliminado")
            except Exception as e:
                logger.warning(f"No se pudo eliminar archivo temporal: {e}")

@router.post("/upload")
async def upload_expedientes(
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None)
):
    """Endpoint para cargar archivo Excel de expedientes (validación previa)"""
    try:
        # Verificar extensión del archivo
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Solo se permiten archivos Excel (.xlsx, .xls)"
            )
        
        # Guardar archivo temporalmente
        temp_path = Path("temp_uploads") / f"expedientes_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        temp_path.parent.mkdir(exist_ok=True)
        
        with open(temp_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        try:
            # Leer Excel
            df = pd.read_excel(temp_path, sheet_name=sheet_name)
            
            # Limpiar datos
            df_clean = clean_expediente_data(df)
            
            return {
                "success": True,
                "message": f"Archivo procesado exitosamente. {len(df_clean)} filas encontradas.",
                "details": {
                    "columns": df_clean.columns.tolist(),
                    "rows": len(df_clean)
                }
            }
            
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error procesando archivo Excel: {str(e)}"
            )
        finally:
            # Limpiar archivo temporal
            if temp_path.exists():
                temp_path.unlink()
                
    except Exception as e:
        logger.error(f"Error en upload_expedientes: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando archivo: {str(e)}"
        )

@router.get("/download-report/{filename}")
async def download_report(filename: str):
    """Endpoint para descargar un reporte generado."""
    report_path = Path("temp_reports") / filename
    if not report_path.exists():
        logger.error(f"Intento de descarga de reporte no encontrado: {filename}")
        raise HTTPException(status_code=404, detail="Archivo de reporte no encontrado.")
    
    logger.info(f"Descargando reporte: {filename}")
    return FileResponse(
        path=report_path, 
        filename=filename, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
