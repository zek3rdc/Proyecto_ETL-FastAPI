#!/usr/bin/env python3
"""
Script para debuggear los cálculos de un funcionario específico
"""

from datetime import datetime, date
import json

# Simular los datos del funcionario COLINA ULACIO RONNY ALEXANDER
funcionario_data = {
    'cedula': '18528163',
    'nombre_completo': 'COLINA ULACIO RONNY ALEXANDER',
    'fecha_ingreso': date(2012, 10, 1),  # 01-10-2012 (fecha ingreso CPNB)
    'fecha_ultimo_ascenso': date(2020, 1, 4),  # 04-01-2020
    'rango_actual': 'PRIMER INSPECTOR',
    'nivel_academico': 'MAGISTER',
    'edad': 38,
    'tiempos_servicio': [
        {
            'fecha_ingreso': '2018-01-01',  # Tiempo de servicio adicional desde 2018
            'fecha_egreso': '2012-09-30',   # Hasta antes del ingreso CPNB
            'institucion': 'Servicio Anterior',
            'cargo': 'Funcionario Policial'
        }
    ]
}
=======
# Simular los datos del funcionario COLINA ULACIO RONNY ALEXANDER
funcionario_data = {
    'cedula': '18528163',
    'nombre_completo': 'COLINA ULACIO RONNY ALEXANDER',
    'fecha_ingreso': date(2012, 10, 1),  # 01-10-2012 (fecha ingreso CPNB)
    'fecha_ultimo_ascenso': date(2020, 1, 4),  # 04-01-2020
    'rango_actual': 'PRIMER INSPECTOR',
    'nivel_academico': 'MAGISTER',
    'edad': 38,
    # Tiempo de servicio adicional desde 2018 (anterior al CPNB)
    'tiempos_servicio': [
        {
            'fecha_ingreso': '2006-01-01',  # Servicio anterior desde 2006
            'fecha_egreso': '20-09-30',   # Hasta antes del ingreso CPNB
            'institucion': 'Servicio Policial Anterior',
            'cargo': 'Funcionario Policial'
        }
    ]
}

# Fecha de corte para el cálculo (asumiendo fecha actual)
fecha_corte = date(2026, 12, 31)

# Criterios de ascenso para PRIMER INSPECTOR
criterios_ascenso = {
    "PRIMER INSPECTOR": {
        "siguienteRango": "INSPECTOR JEFE",
        "tiempoRango": 4,
        "antiguedad": 17,
        "nivelAcademico": "MAGISTER"
    }
}

def calcular_tiempo_servicio(fecha_ingreso: date, fecha_corte: date, tiempos_adicionales: list) -> float:
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

def calcular_antiguedad_policial(fecha_ingreso: date, tiempos_servicio: list, fecha_corte: date) -> str:
    """Calcula la antigüedad total en la función policial como en show.blade.php"""
    if not fecha_ingreso:
        return "0 años, 0 meses, 0 días"
    
    try:
        # Tiempo base desde fecha de ingreso CPNB
        dias_cpnb = (fecha_corte - fecha_ingreso).days
        
        # Tiempo adicional de servicios previos
        dias_adicionales = 0
        for tiempo in tiempos_servicio:
            if tiempo.get('fecha_ingreso') and tiempo.get('fecha_egreso'):
                try:
                    fecha_inicio = datetime.strptime(str(tiempo['fecha_ingreso']), '%Y-%m-%d').date()
                    fecha_fin = datetime.strptime(str(tiempo['fecha_egreso']), '%Y-%m-%d').date()
                    dias_adicionales += (fecha_fin - fecha_inicio).days
                except (ValueError, TypeError):
                    continue
        
        # Total de días combinados
        total_dias = dias_cpnb + dias_adicionales
        
        # Convertir a años, meses y días
        total_anos = total_dias // 365
        dias_restantes = total_dias % 365
        total_meses = dias_restantes // 30
        dias_finales = dias_restantes % 30
        
        return f"{total_anos} años, {total_meses} meses, {dias_finales} días"
        
    except Exception as e:
        return f"Error en cálculo: {e}"

def main():
    print("=== DEBUG FUNCIONARIO COLINA ULACIO RONNY ALEXANDER ===")
    print(f"Cédula: {funcionario_data['cedula']}")
    print(f"Nombre: {funcionario_data['nombre_completo']}")
    print(f"Rango Actual: {funcionario_data['rango_actual']}")
    print(f"Fecha Ingreso: {funcionario_data['fecha_ingreso']}")
    print(f"Fecha Último Ascenso: {funcionario_data['fecha_ultimo_ascenso']}")
    print(f"Fecha Corte: {fecha_corte}")
    print()
    
    # Calcular tiempo de servicio (para evaluación)
    tiempo_servicio_evaluacion = calcular_tiempo_servicio(
        funcionario_data['fecha_ingreso'], 
        fecha_corte, 
        funcionario_data['tiempos_servicio']
    )
    
    # Calcular tiempo en rango (para evaluación)
    tiempo_rango_evaluacion = calcular_tiempo_en_rango(
        funcionario_data['fecha_ultimo_ascenso'],
        funcionario_data['fecha_ingreso'],
        fecha_corte
    )
    
    # Calcular antigüedad policial (para mostrar en Excel)
    antiguedad_policial_display = calcular_antiguedad_policial(
        funcionario_data['fecha_ingreso'],
        funcionario_data['tiempos_servicio'],
        fecha_corte
    )
    
    # Obtener criterios
    criterios = criterios_ascenso.get('PRIMER INSPECTOR', {})
    tiempo_requerido_rango = criterios.get('tiempoRango', 0)
    antiguedad_requerida = criterios.get('antiguedad', 0)
    
    print("=== CÁLCULOS PARA EVALUACIÓN ===")
    print(f"Tiempo de servicio calculado: {tiempo_servicio_evaluacion:.2f} años")
    print(f"Tiempo en rango calculado: {tiempo_rango_evaluacion:.2f} años")
    print()
    
    print("=== CRITERIOS REQUERIDOS ===")
    print(f"Tiempo en rango requerido: {tiempo_requerido_rango} años")
    print(f"Antigüedad requerida: {antiguedad_requerida} años")
    print()
    
    print("=== EVALUACIÓN DE REQUISITOS ===")
    cumple_tiempo_rango = tiempo_rango_evaluacion >= tiempo_requerido_rango
    cumple_antiguedad = tiempo_servicio_evaluacion >= antiguedad_requerida
    
    print(f"¿Cumple tiempo en rango? {cumple_tiempo_rango} ({tiempo_rango_evaluacion:.1f}/{tiempo_requerido_rango})")
    print(f"¿Cumple antigüedad? {cumple_antiguedad} ({tiempo_servicio_evaluacion:.1f}/{antiguedad_requerida})")
    print()
    
    print("=== DATOS MOSTRADOS EN EXCEL ===")
    print(f"Antigüedad en Función Policial: {antiguedad_policial_display}")
    print()
    
    print("=== ANÁLISIS DE DISCREPANCIA ===")
    # Calcular manualmente los años desde las fechas
    anos_desde_ingreso = (fecha_corte - funcionario_data['fecha_ingreso']).days / 365.25
    anos_desde_ultimo_ascenso = (fecha_corte - funcionario_data['fecha_ultimo_ascenso']).days / 365.25
    
    print(f"Años desde ingreso (manual): {anos_desde_ingreso:.2f}")
    print(f"Años desde último ascenso (manual): {anos_desde_ultimo_ascenso:.2f}")
    print()
    
    # Verificar si hay diferencia entre los cálculos
    if abs(tiempo_servicio_evaluacion - anos_desde_ingreso) > 0.1:
        print("⚠️  DISCREPANCIA en tiempo de servicio!")
        print(f"   Función calcular_tiempo_servicio: {tiempo_servicio_evaluacion:.2f}")
        print(f"   Cálculo manual: {anos_desde_ingreso:.2f}")
    
    if abs(tiempo_rango_evaluacion - anos_desde_ultimo_ascenso) > 0.1:
        print("⚠️  DISCREPANCIA en tiempo en rango!")
        print(f"   Función calcular_tiempo_en_rango: {tiempo_rango_evaluacion:.2f}")
        print(f"   Cálculo manual: {anos_desde_ultimo_ascenso:.2f}")
    
    # Verificar si debería cumplir los requisitos
    print()
    print("=== VERIFICACIÓN FINAL ===")
    if anos_desde_ultimo_ascenso >= tiempo_requerido_rango and anos_desde_ingreso >= antiguedad_requerida:
        print("✅ El funcionario DEBERÍA cumplir todos los requisitos")
        if not (cumple_tiempo_rango and cumple_antiguedad):
            print("❌ PERO el sistema dice que NO cumple - HAY UN ERROR")
    else:
        print("❌ El funcionario efectivamente NO cumple los requisitos")
        if not cumple_tiempo_rango:
            print(f"   - Falta tiempo en rango: {anos_desde_ultimo_ascenso:.1f}/{tiempo_requerido_rango}")
        if not cumple_antiguedad:
            print(f"   - Falta antigüedad: {anos_desde_ingreso:.1f}/{antiguedad_requerida}")

if __name__ == "__main__":
    main()
