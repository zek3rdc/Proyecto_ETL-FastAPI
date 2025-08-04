#!/usr/bin/env python3
"""
Script simple para generar listado de ascensos
Uso básico sin argumentos complejos
"""

import requests
import json
import base64
from datetime import date, datetime
import sys

# Configuración
ETL_SERVER_URL = "http://localhost:8001"
ASCENSO_ENDPOINT = f"{ETL_SERVER_URL}/api/ascenso/generar-listado"

def generar_listado_hoy():
    """Genera el listado de ascensos para la fecha actual"""
    fecha_hoy = date.today().strftime('%Y-%m-%d')
    return generar_listado(fecha_hoy)

def generar_listado(fecha_corte):
    """
    Genera el listado de ascensos para la fecha especificada
    
    Args:
        fecha_corte (str): Fecha en formato YYYY-MM-DD
    """
    
    print(f"🚀 Generando listado de ascensos para: {fecha_corte}")
    
    # Verificar servidor
    try:
        health_response = requests.get(f"{ETL_SERVER_URL}/health", timeout=5)
        if health_response.status_code != 200:
            print("❌ El servidor ETL no está disponible")
            print("💡 Ejecuta 'start_etl.bat' para iniciarlo")
            return False
    except:
        print("❌ No se puede conectar al servidor ETL")
        print("💡 Ejecuta 'start_etl.bat' para iniciarlo")
        return False
    
    # Datos de la solicitud
    request_data = {
        "fecha_corte": fecha_corte,
        "incluir_solo_activos": True,
        "incluir_solo_uniformados": True
    }
    
    try:
        print("📡 Procesando solicitud...")
        
        response = requests.post(
            ASCENSO_ENDPOINT,
            json=request_data,
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        
        if response.status_code == 200:
            data = response.json()
            
            print("✅ ¡Listado generado exitosamente!")
            print(f"📊 Total funcionarios evaluados: {data['total_funcionarios_evaluados']}")
            
            # Mostrar estadísticas básicas
            stats = data.get('estadisticas', {})
            print(f"   • Aptos para ascenso: {stats.get('cumple_todos_requisitos', 0)}")
            print(f"   • Pendientes por nivel académico: {stats.get('cumple_menos_academicos', 0)}")
            print(f"   • Con expedientes: {stats.get('expediente_abierto', 0) + stats.get('expediente_cerrado_reciente', 0)}")
            
            # Guardar Excel si está disponible
            if data.get('archivo_excel'):
                try:
                    excel_content = base64.b64decode(data['archivo_excel'])
                    filename = f"listado_ascensos_{fecha_corte.replace('-', '_')}.xlsx"
                    
                    with open(filename, 'wb') as f:
                        f.write(excel_content)
                    
                    print(f"💾 Archivo Excel guardado: {filename}")
                    
                except Exception as e:
                    print(f"⚠️  Error guardando Excel: {e}")
            
            return True
            
        else:
            print(f"❌ Error: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   {error_data.get('detail', 'Error desconocido')}")
            except:
                print(f"   {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Función principal"""
    print("🔧 GENERADOR SIMPLE DE LISTADO DE ASCENSOS")
    print("=" * 45)
    
    if len(sys.argv) > 1:
        # Si se proporciona una fecha como argumento
        fecha = sys.argv[1]
        try:
            # Validar formato de fecha
            datetime.strptime(fecha, '%Y-%m-%d')
            success = generar_listado(fecha)
        except ValueError:
            print("❌ Formato de fecha inválido. Usa: YYYY-MM-DD")
            print("Ejemplo: python generar_ascenso_simple.py 2024-12-31")
            sys.exit(1)
    else:
        # Usar fecha actual
        print("📅 Usando fecha actual...")
        success = generar_listado_hoy()
    
    if success:
        print("\n🎉 ¡Proceso completado!")
    else:
        print("\n💥 Error en el proceso")
        sys.exit(1)

if __name__ == "__main__":
    main()
