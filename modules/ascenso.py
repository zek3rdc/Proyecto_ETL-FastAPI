from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from pathlib import Path
import json
import io
import base64
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import math

# Configuración de logging
logger = logging.getLogger(__name__)

# Importar configuración de base de datos
from config import DATABASE_CONFIG as DB_CONFIG

# Configuración de optimización
MAX_WORKERS = 6  # Máximo 6 hilos
BATCH_SIZE = 3000  # Lotes de 3000 registros

router = APIRouter()

# Definición de rangos (jerarquía académica)
NIVELES_ACADEMICOS = {
    'BACHILLER': 1,
    'T.S.U': 2,
    'LICENCIATURA': 3,
    'ESPECIALIZACIÓN': 4,
    'MAGISTER': 5,
    'DOCTORADO': 6,
    'POSDOCTORADO': 7,
    'Diplomado en Alta Gerencia': 8
}

# Rangos policiales
RANGOS = [
    'AGENTE', 'OFICIAL', 'PRIMER OFICIAL', 'OFICIAL JEFE', 
    'INSPECTOR', 'PRIMER INSPECTOR', 'INSPECTOR JEFE', 
    'COMISARIO', 'PRIMER COMISARIO', 'COMISARIO JEFE', 
    'COMISARIO GENERAL', 'COMISARIO MAYOR', 'COMISARIO SUPERIOR'
]

# Criterios de ascenso
CRITERIOS_ASCENSO = {
    'AGENTE': {
        'siguienteRango': 'OFICIAL',
        'tiempoRango': 2,
        'antiguedad': 2,
        'nivelAcademico': 'BACHILLER'
    },
    'OFICIAL': {
        'siguienteRango': 'PRIMER OFICIAL',
        'tiempoRango': 4,
        'antiguedad': 4,
        'nivelAcademico': 'T.S.U'
    },
    'PRIMER OFICIAL': {
        'siguienteRango': 'OFICIAL JEFE',
        'tiempoRango': 3,
        'antiguedad': 7,
        'nivelAcademico': 'LICENCIATURA'
    },
    'OFICIAL JEFE': {
        'siguienteRango': 'INSPECTOR',
        'tiempoRango': 3,
        'antiguedad': 10,
        'nivelAcademico': 'LICENCIATURA'
    },
    'INSPECTOR': {
        'siguienteRango': 'PRIMER INSPECTOR',
        'tiempoRango': 3,
        'antiguedad': 13,
        'nivelAcademico': 'ESPECIALIZACIÓN'
    },
    'PRIMER INSPECTOR': {
        'siguienteRango': 'INSPECTOR JEFE',
        'tiempoRango': 4,
        'antiguedad': 17,
        'nivelAcademico': 'MAGISTER'
    },
    'INSPECTOR JEFE': {
        'siguienteRango': 'COMISARIO',
        'tiempoRango': 3,
        'antiguedad': 20,
        'nivelAcademico': 'DOCTORADO'
    },
    'COMISARIO': {
        'siguienteRango': 'PRIMER COMISARIO',
        'tiempoRango': 4,
        'antiguedad': 25,
        'nivelAcademico': 'DOCTORADO'
    },
    'PRIMER COMISARIO': {
        'siguienteRango': 'COMISARIO JEFE',
        'tiempoRango': 5,
        'antiguedad': 30,
        'nivelAcademico': 'POSDOCTORADO'
    },
    'COMISARIO JEFE': {
        'siguienteRango': None,
        'tiempoRango': None,
        'antiguedad': None,
        'nivelAcademico': None
    }
}

# Modelos Pydantic
class FuncionarioAscenso(BaseModel):
    id: int
    cedula: str
    nombre_completo: str
    sexo: Optional[str]
    edad: Optional[int]
    nivel_academico: str
    tiempo_en_rango: float
    tiempo_de_servicio: float
    total_puntos: float
    estado_actual: str
    expedientes: str
    rango_actual: str
    rango_a_aplicar: str
    cumple_todos_requisitos: bool
    cumple_requisitos_menos_academicos: bool
    tiene_expediente_cerrado_reciente: bool
    tiene_expediente_abierto: bool
    condicion_actual_invalida: bool
    observaciones: str
    # Nuevos campos para detalles de expedientes
    detalles_expedientes_abiertos: Optional[str] = ""
    detalles_expedientes_cerrados_recientes: Optional[str] = ""
    expedientes_data: Optional[List[Dict]] = []

class ListadoAscensoRequest(BaseModel):
    fecha_corte: date
    incluir_solo_activos: bool = True
    incluir_solo_uniformados: bool = True

class ListadoAscensoResponse(BaseModel):
    fecha_corte: date
    total_funcionarios_evaluados: int
    listas: Dict[str, List[FuncionarioAscenso]]
    estadisticas: Dict[str, int]
    archivo_excel: Optional[str] = None

def get_db_connection():
    """Crear conexión a la base de datos"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

def normalizar_rango(rango: str) -> str:
    """Normaliza el rango actual del funcionario"""
    if not rango or rango.strip() == '':
        return 'NO_REGISTRADO'
    
    rango_limpio = rango.strip().upper()
    
    if rango_limpio not in CRITERIOS_ASCENSO:
        return 'RANGO_INVALIDO'
    
    return rango_limpio

def calcular_edad(fecha_nacimiento: date, fecha_corte: date) -> int:
    """Calcula la edad en años"""
    if not fecha_nacimiento:
        return 0
    
    edad = fecha_corte.year - fecha_nacimiento.year
    if fecha_corte.month < fecha_nacimiento.month or \
       (fecha_corte.month == fecha_nacimiento.month and fecha_corte.day < fecha_nacimiento.day):
        edad -= 1
    
    return edad

def calcular_tiempo_servicio(fecha_ingreso: date, fecha_corte: date, tiempos_adicionales: List[Dict]) -> float:
    """Calcula el tiempo total de servicio en años"""
    if not fecha_ingreso:
        return 0.0
    
    # Tiempo base desde fecha de ingreso
    tiempo_base = (fecha_corte - fecha_ingreso).days / 365.25
    
    # Tiempo adicional de servicios previos
    tiempo_adicional = 0.0
    for tiempo in tiempos_adicionales:
        if tiempo.get('fecha_ingreso') and tiempo.get('fecha_egreso'):
            try:
                fecha_inicio = datetime.strptime(str(tiempo['fecha_ingreso']), '%Y-%m-%d').date()
                fecha_fin = datetime.strptime(str(tiempo['fecha_egreso']), '%Y-%m-%d').date()
                
                # Solo contar si es anterior al ingreso actual o posterior a la fecha actual
                if fecha_inicio < fecha_ingreso or fecha_fin > fecha_corte:
                    tiempo_adicional += (fecha_fin - fecha_inicio).days / 365.25
            except (ValueError, TypeError):
                continue
    
    return tiempo_base + tiempo_adicional

def calcular_tiempo_en_rango(fecha_ultimo_ascenso: date, fecha_ingreso: date, fecha_corte: date) -> float:
    """Calcula el tiempo en el rango actual en años"""
    if fecha_ultimo_ascenso:
        return (fecha_corte - fecha_ultimo_ascenso).days / 365.25
    elif fecha_ingreso:
        return (fecha_corte - fecha_ingreso).days / 365.25
    else:
        return 0.0

def obtener_nivel_academico_maximo(antecedentes_academicos: List[Dict]) -> str:
    """Obtiene el nivel académico más alto del funcionario"""
    if not antecedentes_academicos:
        return 'NO_REGISTRADO'
    
    nivel_maximo = 0
    grado_maximo = 'NO_REGISTRADO'
    
    for antecedente in antecedentes_academicos:
        grado = str(antecedente.get('grado_instruccion', '')).strip().upper()
        if grado in NIVELES_ACADEMICOS:
            if NIVELES_ACADEMICOS[grado] > nivel_maximo:
                nivel_maximo = NIVELES_ACADEMICOS[grado]
                grado_maximo = grado
    
    return grado_maximo

def calcular_puntos_merito(funcionario_data: Dict) -> float:
    """Calcula los puntos de mérito del funcionario"""
    puntos = 0.0
    
    # Puntos por tiempo de servicio (1 punto por año)
    puntos += funcionario_data.get('tiempo_de_servicio', 0)
    
    # Puntos por nivel académico
    nivel_academico = funcionario_data.get('nivel_academico', 'NO_REGISTRADO')
    if nivel_academico in NIVELES_ACADEMICOS:
        puntos += NIVELES_ACADEMICOS[nivel_academico] * 5  # 5 puntos por nivel
    
    # Puntos por tiempo en rango (0.5 puntos por año)
    puntos += funcionario_data.get('tiempo_en_rango', 0) * 0.5
    
    # Bonificación por no tener expedientes (10 puntos)
    if not funcionario_data.get('tiene_expediente_abierto', False) and \
       not funcionario_data.get('tiene_expediente_cerrado_reciente', False):
        puntos += 10
    
    return round(puntos, 2)

def verificar_expedientes(expedientes: List[Dict], fecha_corte: date, tiempo_requerido_anos: int) -> tuple:
    """Verifica el estado de expedientes del funcionario"""
    tiene_expediente_abierto = False
    tiene_expediente_cerrado_reciente = False
    
    for expediente in expedientes:
        estatus = str(expediente.get('estatus', '')).upper()
        fecha_finalizacion = expediente.get('fecha_finalizacion')
        
        if estatus != 'CERRADO' or not fecha_finalizacion:
            tiene_expediente_abierto = True
        else:
            # Verificar si el expediente cerrado es reciente
            if fecha_finalizacion:
                try:
                    fecha_fin = datetime.strptime(str(fecha_finalizacion), '%Y-%m-%d').date()
                    anos_desde_cierre = (fecha_corte - fecha_fin).days / 365.25
                    
                    if anos_desde_cierre < tiempo_requerido_anos:
                        tiene_expediente_cerrado_reciente = True
                except (ValueError, TypeError):
                    continue
    
    return tiene_expediente_abierto, tiene_expediente_cerrado_reciente

def obtener_funcionarios_para_ascenso(fecha_corte: date) -> List[Dict]:
    """Obtiene todos los funcionarios elegibles para evaluación de ascenso"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query principal para obtener funcionarios con todas sus relaciones
        # Se agregan fechas máximas para fecha_ingreso y fecha_ultimo_ascenso considerando otras tablas
        query = """
        SELECT 
            f.id,
            f.cedula,
            f.nombre_completo,
            f.sexo,
            f.fecha_nacimiento,
            -- Fecha ingreso máxima entre funcionarios y tiempos_servicio
            GREATEST(
                COALESCE(f.fecha_ingreso, '1900-01-01'),
                COALESCE(MAX(ts.fecha_ingreso::date), '1900-01-01')
            ) as fecha_ingreso,
            -- Fecha último ascenso máxima entre funcionarios y historial_ascensos
            GREATEST(
                COALESCE(f.fecha_ultimo_ascenso, '1900-01-01'),
                COALESCE(MAX(ha.fecha_ascenso), '1900-01-01')
            ) as fecha_ultimo_ascenso,
            f.rango_actual,
            f.status,
            f.condicion_actual,
            f.tipo,
            f.grado_instruccion,
            
            -- Antecedentes académicos
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'grado_instruccion', aa.grado_instruccion,
                        'institucion', aa.institucion,
                        'fecha_graduacion', aa.fecha_graduacion
                    )
                ) FILTER (WHERE aa.id IS NOT NULL), 
                '[]'::json
            ) as antecedentes_academicos,
            
            -- Tiempo de servicio adicional
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'institucion', ts.institucion,
                        'fecha_ingreso', ts.fecha_ingreso,
                        'fecha_egreso', ts.fecha_egreso,
                        'cargo', ts.cargo
                    )
                ) FILTER (WHERE ts.id IS NOT NULL), 
                '[]'::json
            ) as tiempos_servicio,
            
            -- Expedientes
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'nro_expediente', e.nro_expediente,
                        'fecha_inicio', e.fecha_inicio,
                        'fecha_finalizacion', e.fecha_finalizacion,
                        'estatus', e.estatus,
                        'falta', e.falta,
                        'decision', e.decision,
                        'tipo_expediente', e.tipo_expediente,
                        'tipo_sancion_administrativa', e.tipo_sancion_administrativa
                    )
                ) FILTER (WHERE e.id_exp IS NOT NULL), 
                '[]'::json
            ) as expedientes,
            
            -- Historial de ascensos
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'rango_anterior', ha.rango_anterior,
                        'rango_nuevo', ha.rango_nuevo,
                        'fecha_ascenso', ha.fecha_ascenso,
                        'tipo_ascenso', ha.tipo_ascenso
                    )
                ) FILTER (WHERE ha.id IS NOT NULL), 
                '[]'::json
            ) as historial_ascensos
            
        FROM funcionarios f
        LEFT JOIN antecedentes_academicos aa ON f.id = aa.funcionario_id
        LEFT JOIN tiempo_servicio ts ON f.id = ts.funcionario_id
        LEFT JOIN expedientes e ON f.id = e.funcionario_id
        LEFT JOIN historial_ascensos ha ON f.id = ha.funcionario_id
        WHERE 
            f.status = 'ACTIVO'
            AND UPPER(f.tipo) = 'UNIFORMADO'
            AND UPPER(COALESCE(f.condicion_actual, '')) NOT IN ('SOLICITADO', 'DESTITUIDO', 'PRIVADO DE LIBERTAD')
            AND f.rango_actual IS NOT NULL
            AND f.rango_actual != ''
        GROUP BY f.id, f.fecha_ingreso, f.fecha_ultimo_ascenso
        ORDER BY f.cedula
        """
        
        cursor.execute(query)
        funcionarios = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return [dict(funcionario) for funcionario in funcionarios]
        
    except Exception as e:
        logger.error(f"Error obteniendo funcionarios para ascenso: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo funcionarios: {str(e)}")

def procesar_funcionario_para_ascenso(funcionario_data: Dict, fecha_corte: date) -> FuncionarioAscenso:
    """Procesa un funcionario individual para determinar su elegibilidad de ascenso"""
    
    # Datos básicos - Convertir cedula a string si viene como Decimal
    cedula_raw = funcionario_data.get('cedula', '')
    cedula = str(cedula_raw) if cedula_raw else ''
    nombre_completo = funcionario_data.get('nombre_completo', '')
    sexo = funcionario_data.get('sexo', '')
    fecha_nacimiento = funcionario_data.get('fecha_nacimiento')
    fecha_ingreso = funcionario_data.get('fecha_ingreso')
    fecha_ultimo_ascenso = funcionario_data.get('fecha_ultimo_ascenso')
    rango_actual = funcionario_data.get('rango_actual', '')
    condicion_actual = funcionario_data.get('condicion_actual', '')
    
    # Calcular edad
    edad = calcular_edad(fecha_nacimiento, fecha_corte) if fecha_nacimiento else 0
    
    # Normalizar rango
    rango_normalizado = normalizar_rango(rango_actual)
    
    # Obtener criterios de ascenso
    criterios = CRITERIOS_ASCENSO.get(rango_normalizado, {})
    rango_a_aplicar = criterios.get('siguienteRango') or 'N/A'  # Asegurar que nunca sea None
    # Asegurar que los valores nunca sean None para evitar errores de comparación
    tiempo_requerido_rango = criterios.get('tiempoRango') or 0
    antiguedad_requerida = criterios.get('antiguedad') or 0
    nivel_academico_requerido = criterios.get('nivelAcademico', '')
    
    # Procesar antecedentes académicos
    antecedentes = funcionario_data.get('antecedentes_academicos', [])
    if isinstance(antecedentes, str):
        antecedentes = json.loads(antecedentes)
    nivel_academico = obtener_nivel_academico_maximo(antecedentes)
    
    # Procesar tiempos de servicio
    tiempos_servicio = funcionario_data.get('tiempos_servicio', [])
    if isinstance(tiempos_servicio, str):
        tiempos_servicio = json.loads(tiempos_servicio)
    tiempo_de_servicio = calcular_tiempo_servicio(fecha_ingreso, fecha_corte, tiempos_servicio)
    
    # Calcular tiempo en rango
    tiempo_en_rango = calcular_tiempo_en_rango(fecha_ultimo_ascenso, fecha_ingreso, fecha_corte)
    
    # Procesar expedientes
    expedientes = funcionario_data.get('expedientes', [])
    if isinstance(expedientes, str):
        expedientes = json.loads(expedientes)
    
    tiene_expediente_abierto, tiene_expediente_cerrado_reciente = verificar_expedientes(
        expedientes, fecha_corte, tiempo_requerido_rango
    )
    
    # Verificar cumplimiento de requisitos
    cumple_tiempo_rango = tiempo_en_rango >= tiempo_requerido_rango
    cumple_antiguedad = tiempo_de_servicio >= antiguedad_requerida
    
    # Corregir lógica académica: puede tener nivel igual o superior al requerido
    cumple_nivel_academico = False
    if nivel_academico_requerido and nivel_academico_requerido in NIVELES_ACADEMICOS:
        if nivel_academico in NIVELES_ACADEMICOS:
            cumple_nivel_academico = NIVELES_ACADEMICOS[nivel_academico] >= NIVELES_ACADEMICOS[nivel_academico_requerido]
    elif not nivel_academico_requerido:  # Si no hay requisito académico específico
        cumple_nivel_academico = True
    
    # Verificar que no tenga expedientes que lo descalifiquen
    sin_expedientes_descalificantes = not tiene_expediente_abierto and not tiene_expediente_cerrado_reciente
    
    cumple_todos_requisitos = (cumple_tiempo_rango and cumple_antiguedad and cumple_nivel_academico and sin_expedientes_descalificantes)
    
    cumple_requisitos_menos_academicos = (cumple_tiempo_rango and cumple_antiguedad and sin_expedientes_descalificantes)
    
    # Verificar condición actual inválida
    condiciones_invalidas = ['SOLICITADO', 'DESTITUIDO', 'PRIVADO DE LIBERTAD']
    condicion_actual_invalida = str(condicion_actual).upper() in condiciones_invalidas
    
    # Calcular datos para el funcionario
    funcionario_procesado = {
        'tiempo_de_servicio': tiempo_de_servicio,
        'tiempo_en_rango': tiempo_en_rango,
        'nivel_academico': nivel_academico,
        'tiene_expediente_abierto': tiene_expediente_abierto,
        'tiene_expediente_cerrado_reciente': tiene_expediente_cerrado_reciente
    }
    
    # Calcular puntos de mérito
    total_puntos = calcular_puntos_merito(funcionario_procesado)
    
    # Procesar detalles de expedientes
    detalles_expedientes_abiertos = []
    detalles_expedientes_cerrados_recientes = []
    
    for expediente in expedientes:
        estatus = str(expediente.get('estatus', '')).upper()
        fecha_finalizacion = expediente.get('fecha_finalizacion')
        nro_expediente = expediente.get('nro_expediente', 'N/A')
        falta = expediente.get('falta', 'N/A')
        decision = expediente.get('decision', 'N/A')
        tipo_expediente = expediente.get('tipo_expediente', 'N/A')
        fecha_inicio = expediente.get('fecha_inicio', 'N/A')
        
        detalle = f"Exp: {nro_expediente}, Tipo: {tipo_expediente}, Falta: {falta}, Decisión: {decision}, Inicio: {fecha_inicio}"
        
        if estatus != 'CERRADO' or not fecha_finalizacion:
            detalles_expedientes_abiertos.append(detalle)
        else:
            # Verificar si el expediente cerrado es reciente
            if fecha_finalizacion:
                try:
                    fecha_fin = datetime.strptime(str(fecha_finalizacion), '%Y-%m-%d').date()
                    anos_desde_cierre = (fecha_corte - fecha_fin).days / 365.25
                    
                    if anos_desde_cierre < tiempo_requerido_rango:
                        detalle_cerrado = f"{detalle}, Cierre: {fecha_finalizacion}, Años desde cierre: {anos_desde_cierre:.1f}"
                        detalles_expedientes_cerrados_recientes.append(detalle_cerrado)
                except (ValueError, TypeError):
                    continue
    
    # Determinar estado de expedientes para mostrar
    if tiene_expediente_abierto:
        estado_expedientes = "EXPEDIENTE ABIERTO"
    elif tiene_expediente_cerrado_reciente:
        estado_expedientes = "EXPEDIENTE CERRADO RECIENTE"
    else:
        estado_expedientes = "SIN EXPEDIENTES"
    
    # Generar observaciones
    observaciones = []
    if not cumple_tiempo_rango:
        observaciones.append(f"Falta tiempo en rango: {tiempo_en_rango:.1f}/{tiempo_requerido_rango} años")
    if not cumple_antiguedad:
        observaciones.append(f"Falta antigüedad: {tiempo_de_servicio:.1f}/{antiguedad_requerida} años")
    if not cumple_nivel_academico:
        observaciones.append(f"Nivel académico insuficiente: {nivel_academico} < {nivel_academico_requerido}")
    if tiene_expediente_abierto:
        observaciones.append("Tiene expediente abierto")
    if tiene_expediente_cerrado_reciente:
        observaciones.append("Expediente cerrado reciente")
    if condicion_actual_invalida:
        observaciones.append(f"Condición actual inválida: {condicion_actual}")
    
    return FuncionarioAscenso(
        id=funcionario_data.get('id'),
        cedula=cedula,
        nombre_completo=nombre_completo,
        sexo=sexo,
        edad=edad,
        nivel_academico=nivel_academico,
        tiempo_en_rango=round(tiempo_en_rango, 2),
        tiempo_de_servicio=round(tiempo_de_servicio, 2),
        total_puntos=total_puntos,
        estado_actual=funcionario_data.get('status', ''),
        expedientes=estado_expedientes,
        rango_actual=rango_actual,
        rango_a_aplicar=rango_a_aplicar,
        cumple_todos_requisitos=cumple_todos_requisitos,
        cumple_requisitos_menos_academicos=cumple_requisitos_menos_academicos,
        tiene_expediente_cerrado_reciente=tiene_expediente_cerrado_reciente,
        tiene_expediente_abierto=tiene_expediente_abierto,
        condicion_actual_invalida=condicion_actual_invalida,
        observaciones="; ".join(observaciones) if observaciones else "Cumple todos los requisitos",
        # Nuevos campos con detalles de expedientes
        detalles_expedientes_abiertos="; ".join(detalles_expedientes_abiertos) if detalles_expedientes_abiertos else "",
        detalles_expedientes_cerrados_recientes="; ".join(detalles_expedientes_cerrados_recientes) if detalles_expedientes_cerrados_recientes else "",
        expedientes_data=expedientes
    )

def organizar_listas_ascenso(funcionarios_procesados: List[FuncionarioAscenso]) -> Dict[str, List[FuncionarioAscenso]]:
    """Organiza los funcionarios en las diferentes listas según los criterios"""
    
    listas = {
        "cumple_todos_requisitos": [],
        "cumple_menos_academicos": [],
        "expediente_cerrado_reciente": [],
        "expediente_abierto": [],
        "condicion_actual_invalida": []
    }
    
    for funcionario in funcionarios_procesados:
        # Primera prioridad: Cumple todos los requisitos
        if funcionario.cumple_todos_requisitos:
            listas["cumple_todos_requisitos"].append(funcionario)
        
        # Segunda prioridad: Cumple todos menos académicos
        elif funcionario.cumple_requisitos_menos_academicos:
            listas["cumple_menos_academicos"].append(funcionario)
        
        # Tercera prioridad: Expediente cerrado reciente
        elif funcionario.tiene_expediente_cerrado_reciente:
            listas["expediente_cerrado_reciente"].append(funcionario)
        
        # Cuarta prioridad: Expediente abierto
        elif funcionario.tiene_expediente_abierto:
            listas["expediente_abierto"].append(funcionario)
        
        # Quinta prioridad: Condición actual inválida
        elif funcionario.condicion_actual_invalida:
            listas["condicion_actual_invalida"].append(funcionario)
        
        # Si no cumple ninguna categoría específica, va a expediente cerrado
        else:
            listas["expediente_cerrado_reciente"].append(funcionario)
    
    # Ordenar cada lista por puntos de mérito (descendente)
    for lista_nombre in listas:
        listas[lista_nombre].sort(key=lambda x: x.total_puntos, reverse=True)
    
    return listas

def generar_excel_ascensos(listas: Dict[str, List[FuncionarioAscenso]], fecha_corte: date) -> str:
    """Genera un archivo Excel con las listas de ascenso"""
    
    try:
        # Crear un buffer en memoria para el archivo Excel
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            
            # Hoja 1: Todos los funcionarios organizados por categorías
            datos_completos = []
            
            for categoria, funcionarios in listas.items():
                for funcionario in funcionarios:
                    datos_completos.append({
                        'Categoría': categoria.replace('_', ' ').title(),
                        'Cédula': funcionario.cedula,
                        'Nombre Completo': funcionario.nombre_completo,
                        'Sexo': funcionario.sexo,
                        'Edad': funcionario.edad,
                        'Nivel Académico': funcionario.nivel_academico,
                        'Tiempo en Rango (años)': funcionario.tiempo_en_rango,
                        'Tiempo de Servicio (años)': funcionario.tiempo_de_servicio,
                        'Total Puntos': funcionario.total_puntos,
                        'Estado Actual': funcionario.estado_actual,
                        'Expedientes': funcionario.expedientes,
                        'Rango Actual': funcionario.rango_actual,
                        'Rango a Aplicar': funcionario.rango_a_aplicar,
                        'Detalles Expedientes Cerrados Recientes': funcionario.detalles_expedientes_cerrados_recientes,
                        'Detalles Expedientes Abiertos': funcionario.detalles_expedientes_abiertos,
                        'Observaciones': funcionario.observaciones
                    })
            
            if datos_completos:
                df_completo = pd.DataFrame(datos_completos)
                df_completo.to_excel(writer, sheet_name='Listado Completo', index=False)
                
                # Agregar autofiltro y ajustar ancho columnas
                worksheet = writer.sheets['Listado Completo']
                worksheet.auto_filter.ref = worksheet.dimensions
                for col_cells in worksheet.columns:
                    max_length = 0
                    column = col_cells[0].column_letter
                    for cell in col_cells:
                        try:
                            if cell.value:
                                max_length = max(max_length, len(str(cell.value)))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column].width = adjusted_width
            
            # Hoja 2: Solo los que cumplen todos los requisitos
            if listas["cumple_todos_requisitos"]:
                datos_elegibles = []
                for funcionario in listas["cumple_todos_requisitos"]:
                    datos_elegibles.append({
                        'Cédula': funcionario.cedula,
                        'Nombre Completo': funcionario.nombre_completo,
                        'Sexo': funcionario.sexo,
                        'Edad': funcionario.edad,
                        'Nivel Académico': funcionario.nivel_academico,
                        'Tiempo en Rango (años)': funcionario.tiempo_en_rango,
                        'Tiempo de Servicio (años)': funcionario.tiempo_de_servicio,
                        'Total Puntos': funcionario.total_puntos,
                        'Rango Actual': funcionario.rango_actual,
                        'Rango a Aplicar': funcionario.rango_a_aplicar,
                        'Condición Actual': funcionario.estado_actual,
                        'Detalles Expedientes Cerrados Recientes': funcionario.detalles_expedientes_cerrados_recientes,
                        'Detalles Expedientes Abiertos': funcionario.detalles_expedientes_abiertos
                    })
                
                df_elegibles = pd.DataFrame(datos_elegibles)
                df_elegibles.to_excel(writer, sheet_name='Elegibles Completos', index=False)
                
                # Agregar autofiltro y ajustar ancho columnas
                worksheet = writer.sheets['Elegibles Completos']
                worksheet.auto_filter.ref = worksheet.dimensions
                for col_cells in worksheet.columns:
                    max_length = 0
                    column = col_cells[0].column_letter
                    for cell in col_cells:
                        try:
                            if cell.value:
                                max_length = max(max_length, len(str(cell.value)))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column].width = adjusted_width
            
            # Hoja 3: Solo los que cumplen menos académicos
            if listas["cumple_menos_academicos"]:
                datos_menos_academicos = []
                for funcionario in listas["cumple_menos_academicos"]:
                    datos_menos_academicos.append({
                        'Cédula': funcionario.cedula,
                        'Nombre Completo': funcionario.nombre_completo,
                        'Sexo': funcionario.sexo,
                        'Edad': funcionario.edad,
                        'Nivel Académico': funcionario.nivel_academico,
                        'Tiempo en Rango (años)': funcionario.tiempo_en_rango,
                        'Tiempo de Servicio (años)': funcionario.tiempo_de_servicio,
                        'Total Puntos': funcionario.total_puntos,
                        'Rango Actual': funcionario.rango_actual,
                        'Rango a Aplicar': funcionario.rango_a_aplicar,
                        'Observaciones': funcionario.observaciones
                    })
                
                df_menos_academicos = pd.DataFrame(datos_menos_academicos)
                df_menos_academicos.to_excel(writer, sheet_name='Elegibles Menos Académicos', index=False)
                
                # Agregar autofiltro y ajustar ancho columnas
                worksheet = writer.sheets['Elegibles Menos Académicos']
                worksheet.auto_filter.ref = worksheet.dimensions
                for col_cells in worksheet.columns:
                    max_length = 0
                    column = col_cells[0].column_letter
                    for cell in col_cells:
                        try:
                            if cell.value:
                                max_length = max(max_length, len(str(cell.value)))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column].width = adjusted_width
        
        # Obtener el contenido del buffer
        output.seek(0)
        excel_content = output.getvalue()
        
        # Codificar en base64 para envío
        excel_base64 = base64.b64encode(excel_content).decode('utf-8')
        
        return excel_base64
        
    except Exception as e:
        logger.error(f"Error generando Excel: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando archivo Excel: {str(e)}")

# Endpoints

@router.get("/ascenso/criterios")
async def get_ascenso_criterios():
    """
    Endpoint para obtener los criterios de ascenso, rangos y niveles académicos.
    """
    return {
        "nivelesAcademicos": NIVELES_ACADEMICOS,
        "rangos": RANGOS,
        "criteriosAscenso": CRITERIOS_ASCENSO
    }

def procesar_lote_funcionarios(lote_funcionarios: List[Dict], fecha_corte: date, lote_id: int) -> List[FuncionarioAscenso]:
    """Procesa un lote de funcionarios en un hilo separado"""
    start_time = time.time()
    funcionarios_procesados = []
    
    logger.info(f"[LOTE_{lote_id}] Iniciando procesamiento de {len(lote_funcionarios)} funcionarios")
    
    for funcionario_data in lote_funcionarios:
        try:
            funcionario_procesado = procesar_funcionario_para_ascenso(funcionario_data, fecha_corte)
            funcionarios_procesados.append(funcionario_procesado)
        except Exception as e:
            logger.error(f"[LOTE_{lote_id}] Error procesando funcionario {funcionario_data.get('cedula', 'N/A')}: {e}")
            continue
    
    processing_time = time.time() - start_time
    logger.info(f"[LOTE_{lote_id}] Completado en {processing_time:.2f}s - {len(funcionarios_procesados)} funcionarios procesados")
    
    return funcionarios_procesados

def procesar_funcionarios_multihilo(funcionarios_data: List[Dict], fecha_corte: date) -> List[FuncionarioAscenso]:
    """Procesa funcionarios usando múltiples hilos y lotes"""
    total_funcionarios = len(funcionarios_data)
    
    if total_funcionarios == 0:
        return []
    
    # Calcular número de lotes
    num_lotes = math.ceil(total_funcionarios / BATCH_SIZE)
    
    logger.info(f"[MULTIHILO] Procesando {total_funcionarios} funcionarios en {num_lotes} lotes de {BATCH_SIZE}")
    logger.info(f"[MULTIHILO] Usando máximo {MAX_WORKERS} hilos")
    
    # Dividir funcionarios en lotes
    lotes = []
    for i in range(0, total_funcionarios, BATCH_SIZE):
        lote = funcionarios_data[i:i + BATCH_SIZE]
        lotes.append(lote)
    
    # Procesar lotes en paralelo
    funcionarios_procesados = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Enviar lotes a los hilos
        future_to_lote = {}
        for lote_id, lote in enumerate(lotes):
            future = executor.submit(procesar_lote_funcionarios, lote, fecha_corte, lote_id)
            future_to_lote[future] = lote_id
        
        # Recopilar resultados
        for future in as_completed(future_to_lote):
            lote_id = future_to_lote[future]
            try:
                resultado_lote = future.result()
                funcionarios_procesados.extend(resultado_lote)
                logger.info(f"[MULTIHILO] Lote {lote_id} completado - {len(resultado_lote)} funcionarios")
            except Exception as e:
                logger.error(f"[MULTIHILO] Error en lote {lote_id}: {e}")
    
    total_time = time.time() - start_time
    velocidad = len(funcionarios_procesados) / total_time if total_time > 0 else 0
    
    logger.info(f"[MULTIHILO] Procesamiento completado en {total_time:.2f}s")
    logger.info(f"[MULTIHILO] Velocidad: {velocidad:.2f} funcionarios/segundo")
    logger.info(f"[MULTIHILO] Total procesados: {len(funcionarios_procesados)}/{total_funcionarios}")
    
    return funcionarios_procesados

@router.post("/ascenso/generar-listado")
async def generar_listado_ascenso(request: ListadoAscensoRequest) -> ListadoAscensoResponse:
    """
    Genera el listado de ascensos según la fecha de corte especificada.
    Utiliza procesamiento multihilo y por lotes para optimizar el rendimiento.
    """
    try:
        logger.info(f"Generando listado de ascensos para fecha: {request.fecha_corte}")
        logger.info(f"Configuración: MAX_WORKERS={MAX_WORKERS}, BATCH_SIZE={BATCH_SIZE}")
        
        # Obtener funcionarios de la base de datos
        funcionarios_data = obtener_funcionarios_para_ascenso(request.fecha_corte)
        
        logger.info(f"Obtenidos {len(funcionarios_data)} funcionarios para evaluar")
        
        # Procesar funcionarios usando multihilo y lotes
        if len(funcionarios_data) >= BATCH_SIZE:
            logger.info("Usando procesamiento multihilo optimizado")
            funcionarios_procesados = procesar_funcionarios_multihilo(funcionarios_data, request.fecha_corte)
        else:
            logger.info("Usando procesamiento secuencial (pocos funcionarios)")
            funcionarios_procesados = []
            for funcionario_data in funcionarios_data:
                try:
                    funcionario_procesado = procesar_funcionario_para_ascenso(funcionario_data, request.fecha_corte)
                    funcionarios_procesados.append(funcionario_procesado)
                except Exception as e:
                    logger.error(f"Error procesando funcionario {funcionario_data.get('cedula', 'N/A')}: {e}")
                    continue
        
        logger.info(f"Procesados {len(funcionarios_procesados)} funcionarios exitosamente")
        
        # Organizar en listas
        listas = organizar_listas_ascenso(funcionarios_procesados)
        
        # Calcular estadísticas
        estadisticas = {
            "total_evaluados": len(funcionarios_procesados),
            "cumple_todos_requisitos": len(listas["cumple_todos_requisitos"]),
            "cumple_menos_academicos": len(listas["cumple_menos_academicos"]),
            "expediente_cerrado_reciente": len(listas["expediente_cerrado_reciente"]),
            "expediente_abierto": len(listas["expediente_abierto"]),
            "condicion_actual_invalida": len(listas["condicion_actual_invalida"])
        }
        
        # Generar archivo Excel
        try:
            archivo_excel = generar_excel_ascensos(listas, request.fecha_corte)
        except Exception as e:
            logger.error(f"Error generando Excel: {e}")
            archivo_excel = None
        
        logger.info(f"Listado generado exitosamente. Estadísticas: {estadisticas}")
        
        return ListadoAscensoResponse(
            fecha_corte=request.fecha_corte,
            total_funcionarios_evaluados=len(funcionarios_procesados),
            listas=listas,
            estadisticas=estadisticas,
            archivo_excel=archivo_excel
        )
        
    except Exception as e:
        logger.error(f"Error generando listado de ascensos: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando listado: {str(e)}")

@router.get("/ascenso/funcionarios-por-rango")
async def get_funcionarios_por_rango(rango: str = Query(..., description="Rango a consultar")):
    """
    Obtiene la cantidad de funcionarios por rango específico.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            COUNT(*) as total,
            rango_actual
        FROM funcionarios 
        WHERE 
            status = 'ACTIVO'
            AND UPPER(tipo) = 'UNIFORMADO'
            AND UPPER(COALESCE(condicion_actual, '')) NOT IN ('SOLICITADO', 'DESTITUIDO', 'PRIVADO DE LIBERTAD')
            AND UPPER(rango_actual) = UPPER(%s)
        GROUP BY rango_actual
        """
        
        cursor.execute(query, (rango,))
        resultado = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if resultado:
            return {
                "rango": resultado['rango_actual'],
                "total_funcionarios": resultado['total'],
                "criterios_ascenso": CRITERIOS_ASCENSO.get(rango.upper(), {})
            }
        else:
            return {
                "rango": rango,
                "total_funcionarios": 0,
                "criterios_ascenso": CRITERIOS_ASCENSO.get(rango.upper(), {})
            }
            
    except Exception as e:
        logger.error(f"Error obteniendo funcionarios por rango: {e}")
        raise HTTPException(status_code=500, detail=f"Error consultando funcionarios: {str(e)}")

@router.get("/ascenso/estadisticas-generales")
async def get_estadisticas_generales():
    """
    Obtiene estadísticas generales de funcionarios por rango.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            rango_actual,
            COUNT(*) as total,
            COUNT(CASE WHEN sexo = 'M' THEN 1 END) as masculino,
            COUNT(CASE WHEN sexo = 'F' THEN 1 END) as femenino,
            AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, fecha_nacimiento))) as edad_promedio,
            AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, fecha_ingreso))) as antiguedad_promedio
        FROM funcionarios 
        WHERE 
            status = 'ACTIVO'
            AND UPPER(tipo) = 'UNIFORMADO'
            AND UPPER(COALESCE(condicion_actual, '')) NOT IN ('SOLICITADO', 'DESTITUIDO', 'PRIVADO DE LIBERTAD')
            AND rango_actual IS NOT NULL
            AND rango_actual != ''
        GROUP BY rango_actual
        ORDER BY 
            CASE rango_actual
                WHEN 'AGENTE' THEN 1
                WHEN 'OFICIAL' THEN 2
                WHEN 'PRIMER OFICIAL' THEN 3
                WHEN 'OFICIAL JEFE' THEN 4
                WHEN 'INSPECTOR' THEN 5
                WHEN 'PRIMER INSPECTOR' THEN 6
                WHEN 'INSPECTOR JEFE' THEN 7
                WHEN 'COMISARIO' THEN 8
                WHEN 'PRIMER COMISARIO' THEN 9
                WHEN 'COMISARIO JEFE' THEN 10
                ELSE 11
            END
        """
        
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        estadisticas = []
        total_general = 0
        
        for resultado in resultados:
            total_general += resultado['total']
            estadisticas.append({
                "rango": resultado['rango_actual'],
                "total": resultado['total'],
                "masculino": resultado['masculino'] or 0,
                "femenino": resultado['femenino'] or 0,
                "edad_promedio": round(float(resultado['edad_promedio'] or 0), 1),
                "antiguedad_promedio": round(float(resultado['antiguedad_promedio'] or 0), 1),
                "criterios_ascenso": CRITERIOS_ASCENSO.get(resultado['rango_actual'], {})
            })
        
        return {
            "total_funcionarios": total_general,
            "estadisticas_por_rango": estadisticas,
            "fecha_consulta": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas generales: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo estadísticas: {str(e)}")

@router.post("/ascenso/simular-ascenso")
async def simular_ascenso(cedula: str, fecha_simulacion: date):
    """
    Simula el ascenso de un funcionario específico en una fecha determinada.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Obtener datos del funcionario específico
        query = """
        SELECT 
            f.id,
            f.cedula,
            f.nombre_completo,
            f.sexo,
            f.fecha_nacimiento,
            f.fecha_ingreso,
            f.fecha_ultimo_ascenso,
            f.rango_actual,
            f.status,
            f.condicion_actual,
            f.tipo,
            f.grado_instruccion,
            
            -- Antecedentes académicos
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'grado_instruccion', aa.grado_instruccion,
                        'institucion', aa.institucion,
                        'fecha_graduacion', aa.fecha_graduacion
                    )
                ) FILTER (WHERE aa.id IS NOT NULL), 
                '[]'::json
            ) as antecedentes_academicos,
            
            -- Tiempo de servicio adicional
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'institucion', ts.institucion,
                        'fecha_ingreso', ts.fecha_ingreso,
                        'fecha_egreso', ts.fecha_egreso,
                        'cargo', ts.cargo
                    )
                ) FILTER (WHERE ts.id IS NOT NULL), 
                '[]'::json
            ) as tiempos_servicio,
            
            -- Expedientes
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'nro_expediente', e.nro_expediente,
                        'fecha_inicio', e.fecha_inicio,
                        'fecha_finalizacion', e.fecha_finalizacion,
                        'estatus', e.estatus,
                        'falta', e.falta,
                        'decision', e.decision
                    )
                ) FILTER (WHERE e.id_exp IS NOT NULL), 
                '[]'::json
            ) as expedientes
            
        FROM funcionarios f
        LEFT JOIN antecedentes_academicos aa ON f.id = aa.funcionario_id
        LEFT JOIN tiempo_servicio ts ON f.id = ts.funcionario_id
        LEFT JOIN expedientes e ON f.id = e.funcionario_id
        WHERE f.cedula = %s
        GROUP BY f.id
        """
        
        cursor.execute(query, (cedula,))
        funcionario_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not funcionario_data:
            raise HTTPException(status_code=404, detail="Funcionario no encontrado")
        
        # Procesar funcionario para la fecha de simulación
        funcionario_procesado = procesar_funcionario_para_ascenso(dict(funcionario_data), fecha_simulacion)
        
        return {
            "funcionario": funcionario_procesado,
            "fecha_simulacion": fecha_simulacion,
            "es_elegible": funcionario_procesado.cumple_todos_requisitos,
            "recomendaciones": {
                "puede_ascender": funcionario_procesado.cumple_todos_requisitos,
                "requisitos_faltantes": funcionario_procesado.observaciones if not funcionario_procesado.cumple_todos_requisitos else "Ninguno",
                "puntos_merito": funcionario_procesado.total_puntos,
                "rango_destino": funcionario_procesado.rango_a_aplicar
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error simulando ascenso: {e}")
        raise HTTPException(status_code=500, detail=f"Error en simulación: {str(e)}")

@router.get("/ascenso/exportar-excel/{fecha_corte}")
async def exportar_excel_ascensos(fecha_corte: date):
    """
    Exporta directamente el archivo Excel de ascensos para una fecha específica.
    """
    try:
        # Crear request para generar listado
        request = ListadoAscensoRequest(fecha_corte=fecha_corte)
        
        # Generar listado
        listado = await generar_listado_ascenso(request)
        
        if not listado.archivo_excel:
            raise HTTPException(status_code=500, detail="No se pudo generar el archivo Excel")
        
        # Decodificar el archivo base64
        excel_content = base64.b64decode(listado.archivo_excel)
        
        # Crear respuesta con el archivo
        from fastapi.responses import Response
        
        filename = f"listado_ascensos_{fecha_corte.strftime('%Y_%m_%d')}.xlsx"
        
        return Response(
            content=excel_content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exportando Excel: {e}")
        raise HTTPException(status_code=500, detail=f"Error exportando archivo: {str(e)}")
