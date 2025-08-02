import pandas as pd
from fastapi import HTTPException
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

def detect_file_type(filename: str) -> str:
    """Detectar tipo de archivo por extensión"""
    ext = filename.lower().split('.')[-1]
    if ext in ['xlsx', 'xls']:
        return 'excel'
    elif ext == 'csv':
        return 'csv'
    else:
        raise HTTPException(status_code=400, detail="Tipo de archivo no soportado")

def read_excel_sheets(file_path: str) -> List[str]:
    """Obtener lista de hojas de un archivo Excel, asegurando que el archivo se cierre."""
    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()
        # Pandas ahora opera sobre los bytes en memoria, liberando el archivo en disco.
        excel_file = pd.ExcelFile(file_bytes)
        return excel_file.sheet_names
    except Exception as e:
        logger.error(f"Error leyendo hojas de Excel: {e}")
        raise HTTPException(status_code=400, detail="Error leyendo archivo Excel")

def read_file_data(file_path: str, file_type: str, sheet_name: Optional[str] = None, encoding: str = 'latin1') -> pd.DataFrame:
    """Leer datos del archivo, asegurando que el archivo se cierre para evitar bloqueos."""
    try:
        if file_type == 'excel':
            with open(file_path, 'rb') as f:
                file_bytes = f.read()
            # Pandas ahora opera sobre los bytes en memoria, liberando el archivo en disco.
            df = pd.read_excel(file_bytes, sheet_name=sheet_name)
        elif file_type == 'csv':
            # Aunque menos común con CSV, ser explícito no hace daño.
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline()
                if ';' in first_line:
                    separator = ';'
                elif ',' in first_line:
                    separator = ','
                elif '\t' in first_line:
                    separator = '\t'
                else:
                    separator = ','
            
            df = pd.read_csv(file_path, sep=separator, encoding=encoding)
        
        df.columns = df.columns.astype(str).str.strip()
        
        return df
    except Exception as e:
        logger.error(f"Error leyendo archivo: {e}")
        raise HTTPException(status_code=400, detail=f"Error leyendo archivo: {str(e)}")
