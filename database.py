import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException
import logging
from typing import List, Dict
from config import DATABASE_CONFIG as DB_CONFIG

logger = logging.getLogger(__name__)

def get_db_connection():
    """Crear conexión a la base de datos"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

def get_database_tables() -> List[Dict]:
    """Obtener lista de tablas disponibles en la base de datos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT table_name, 
                   COALESCE(obj_description(c.oid), table_name) as display_name
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = []
        for row in cursor.fetchall():
            tables.append({
                'name': row['table_name'],
                'display_name': row['display_name']
            })
        
        cursor.close()
        conn.close()
        
        return tables
    except Exception as e:
        logger.error(f"Error obteniendo tablas: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo tablas de la base de datos")

def get_table_columns_info(table_name: str) -> List[Dict]:
    """Obtener información detallada de las columnas de una tabla incluyendo foreign keys"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        logger.info(f"[FK_DEBUG] Obteniendo columnas para tabla: {table_name}")
        
        cursor.execute("""
            WITH key_columns AS (
                SELECT DISTINCT ON (a.attname)
                    a.attname as column_name,
                    CASE 
                        WHEN i.indisprimary THEN 'PRIMARY KEY'
                        WHEN i.indisunique THEN 'UNIQUE'
                        ELSE NULL
                    END as key_type
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid 
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass 
                    AND (i.indisprimary OR i.indisunique)
                
                UNION
                
                SELECT DISTINCT
                    kcu.column_name,
                    'UNIQUE' as key_type
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = %s
                    AND tc.table_schema = 'public'
                    AND tc.constraint_type = 'UNIQUE'
            ), foreign_keys AS (
                SELECT DISTINCT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = %s
                    AND tc.table_schema = 'public'
            ), column_info AS (
                SELECT 
                    cols.column_name as name,
                    cols.data_type,
                    cols.is_nullable = 'NO' as required,
                    cols.column_default,
                    cols.character_maximum_length,
                    cols.numeric_precision,
                    cols.numeric_scale,
                    COALESCE(pgd.description, '') as description,
                    CASE 
                        WHEN cols.data_type = 'character varying' THEN 'text'
                        WHEN cols.data_type = 'timestamp without time zone' THEN 'datetime'
                        WHEN cols.data_type = 'double precision' THEN 'number'
                        WHEN cols.data_type = 'integer' THEN 'number'
                        WHEN cols.data_type = 'boolean' THEN 'boolean'
                        ELSE cols.data_type
                    END as type,
                    kc.key_type,
                    fk.foreign_table_name,
                    fk.foreign_column_name,
                    fk.constraint_name as fk_constraint_name
                FROM information_schema.columns cols
                LEFT JOIN pg_class pgc ON pgc.relname = cols.table_name
                LEFT JOIN pg_description pgd ON pgd.objoid = pgc.oid 
                    AND pgd.objsubid = cols.ordinal_position
                LEFT JOIN key_columns kc ON kc.column_name = cols.column_name
                LEFT JOIN foreign_keys fk ON fk.column_name = cols.column_name
                WHERE cols.table_name = %s 
                AND cols.table_schema = 'public'
                AND cols.column_name NOT IN ('created_at', 'updated_at')
            )
            SELECT 
                name,
                data_type,
                required,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                description,
                type,
                key_type,
                foreign_table_name,
                foreign_column_name,
                fk_constraint_name
            FROM column_info
            ORDER BY name
        """, (table_name, table_name, table_name, table_name))
        
        main_results = cursor.fetchall()
        
        columns = []
        for row in main_results:
            column_data = {
                'name': row['name'],
                'type': row['type'],
                'required': row['required'],
                'description': row['description'],
                'default': row['column_default'],
                'max_length': row['character_maximum_length'],
                'precision': row['numeric_precision'],
                'scale': row['numeric_scale'],
                'key_type': row['key_type'],
                'is_key': row['key_type'] is not None,
                'foreign_table': row['foreign_table_name'],
                'foreign_column': row['foreign_column_name'],
                'fk_constraint': row['fk_constraint_name'],
                'has_foreign_key': row['foreign_table_name'] is not None
            }
            columns.append(column_data)
        
        cursor.close()
        conn.close()
        
        return columns
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo columnas: {str(e)}")

def get_foreign_table_columns_info(table_name: str) -> List[Dict]:
    """Obtener columnas de una tabla relacionada que pueden usarse como referencia"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        logger.info(f"[FK_DEBUG] Obteniendo columnas de tabla relacionada: {table_name}")
        
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name = %s AND table_schema = 'public'
        """, (table_name,))
        
        if not cursor.fetchone():
            logger.error(f"[FK_DEBUG] Tabla {table_name} no encontrada en esquema public")
            raise HTTPException(status_code=404, detail=f"Tabla {table_name} no encontrada")
        
        cursor.execute("""
            WITH key_columns AS (
                SELECT DISTINCT ON (a.attname)
                    a.attname as column_name,
                    CASE 
                        WHEN i.indisprimary THEN 'PRIMARY KEY'
                        WHEN i.indisunique THEN 'UNIQUE'
                        ELSE NULL
                    END as key_type
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid 
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass 
                    AND (i.indisprimary OR i.indisunique)
                
                UNION
                
                SELECT DISTINCT
                    kcu.column_name,
                    'UNIQUE' as key_type
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = %s
                    AND tc.table_schema = 'public'
                    AND tc.constraint_type = 'UNIQUE'
            )
            SELECT 
                cols.column_name as name,
                cols.data_type,
                CASE 
                    WHEN cols.data_type = 'character varying' THEN 'text'
                    WHEN cols.data_type = 'timestamp without time zone' THEN 'datetime'
                    WHEN cols.data_type = 'double precision' THEN 'number'
                    WHEN cols.data_type = 'integer' THEN 'number'
                    WHEN cols.data_type = 'boolean' THEN 'boolean'
                    ELSE cols.data_type
                END as type,
                COALESCE(pgd.description, '') as description,
                kc.key_type,
                kc.key_type IS NOT NULL as is_key,
                CASE 
                    WHEN kc.key_type IS NOT NULL THEN 1
                    WHEN cols.column_name ILIKE '%codigo%' OR cols.column_name ILIKE '%numero%' 
                         OR cols.column_name ILIKE '%cedula%' OR cols.column_name ILIKE '%nro%'
                         OR cols.column_name ILIKE '%exp%' OR cols.column_name ILIKE '%id%' THEN 2
                    WHEN cols.column_name ILIKE '%nombre%' OR cols.column_name ILIKE '%descripcion%'
                         OR cols.column_name ILIKE '%titulo%' THEN 3
                    ELSE 4
                END as priority
            FROM information_schema.columns cols
            LEFT JOIN pg_class pgc ON pgc.relname = cols.table_name
            LEFT JOIN pg_description pgd ON pgd.objoid = pgc.oid 
                AND pgd.objsubid = cols.ordinal_position
            LEFT JOIN key_columns kc ON kc.column_name = cols.column_name
            WHERE cols.table_name = %s 
            AND cols.table_schema = 'public'
            AND cols.column_name NOT IN ('created_at', 'updated_at')
            ORDER BY priority, cols.column_name
        """, (table_name, table_name, table_name))
        
        columns = []
        for row in cursor.fetchall():
            column_data = {
                'name': row['name'],
                'type': row['type'],
                'description': row['description'],
                'key_type': row['key_type'],
                'is_key': row['is_key'],
                'priority': row['priority'],
                'recommended': row['priority'] <= 2
            }
            columns.append(column_data)
        
        cursor.close()
        conn.close()
        
        return columns
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla relacionada {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo columnas de tabla relacionada: {str(e)}")
