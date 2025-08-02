import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def apply_transformations(df: pd.DataFrame, transformations: Dict) -> pd.DataFrame:
    """Aplicar transformaciones a los datos"""
    df_transformed = df.copy()
    
    for column, transform_config in transformations.items():
        if column not in df_transformed.columns:
            continue
            
        transform_type = transform_config.get('type')
        options = transform_config.get('options', {})
        
        try:
            if transform_type == 'date':
                df_transformed[column] = transform_date_column(df_transformed[column], options)
            elif transform_type == 'number':
                df_transformed[column] = transform_number_column(df_transformed[column], options)
            elif transform_type == 'text':
                df_transformed[column] = transform_text_column(df_transformed[column], options)
            elif transform_type == 'replace':
                df_transformed[column] = transform_replace_column(df_transformed[column], options)
        except Exception as e:
            logger.warning(f"Error transformando columna {column}: {e}")
    
    return df_transformed

def transform_date_column(series: pd.Series, options: Dict) -> pd.Series:
    """Transformar columna de fechas"""
    date_format_from = options.get('date_format_from', 'auto')
    date_format_to = options.get('date_format_to', '%Y-%m-%d')
    
    if date_format_from == 'auto':
        dt_series = pd.to_datetime(series, infer_datetime_format=True, errors='coerce')
    else:
        dt_series = pd.to_datetime(series, format=date_format_from, errors='coerce')
    
    dt_series = dt_series.where(dt_series.notna(), None)
    
    formatted_series = dt_series.dt.strftime(date_format_to)
    formatted_series = formatted_series.where(dt_series.notna(), None)
    
    return formatted_series

def transform_number_column(series: pd.Series, options: Dict) -> pd.Series:
    """Transformar columna numérica"""
    decimal_separator = options.get('decimal_separator', '.')
    
    if decimal_separator == ',':
        series = series.astype(str).str.replace(',', '.')
    
    return pd.to_numeric(series, errors='coerce')

def transform_text_column(series: pd.Series, options: Dict) -> pd.Series:
    """Transformar columna de texto"""
    text_transform = options.get('text_transform', 'none')
    
    series = series.astype(str)
    
    if text_transform == 'upper':
        return series.str.upper()
    elif text_transform == 'lower':
        return series.str.lower()
    elif text_transform == 'title':
        return series.str.title()
    elif text_transform == 'trim':
        return series.str.strip()
    
    return series

def transform_replace_column(series: pd.Series, options: Dict) -> pd.Series:
    """Reemplazar valores en columna"""
    replace_from = options.get('replace_from', '')
    replace_to = options.get('replace_to', '')
    
    if replace_from:
        return series.astype(str).str.replace(replace_from, replace_to)
    
    return series
