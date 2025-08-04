#!/usr/bin/env python3
"""
Script simple para generar solo el archivo Excel de ascensos
Utiliza el módulo ascenso.py y guarda únicamente el archivo Excel
"""

import requests
import base64
from datetime import date, datetime
import sys
import os
from pathlib import Path

# Configuración del servidor ETL
ETL_SERVER_URL = "http://localhost:8001"
ASCENSO_ENDPOINT = f"{ETL_SERVER_URL}/api/ascenso/generar-listado"

def verificar_servidor_activo():
    """Verifica si el servidor ETL está activo"""
    try:
        response = requests.get(f"{ETL_SERVER_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def generar_excel_ascenso(fecha_corte, directorio_salida=None):
    """
    Genera solo el archivo Excel de ascensos
    
    Args:
        fecha_corte (str): Fecha de corte en formato YYYY-MM-DD
        directorio_salida (str): Directorio donde guardar el Excel (opcional)
    """
    
    # Determinar directorio de salida
    if directorio_salida:
        output_dir = Path(directorio_salida)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path.cwd()
    
    print(f"📋 Generando Excel de ascensos para: {fecha_corte}")
    print(f"📁 Directorio de salida: {output_dir.absolute()}")
    print()
    
    # Verificar servidor
    if not verificar_servidor_activo():
        print("❌ El servidor ETL no está disponible")
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
            timeout=300  # 5 minutos
        )
        
        if response.status_code == 200:
            data = response.json()
            
            print("✅ Listado procesado exitosamente")
            print(f"📊 Total funcionarios evaluados: {data['total_funcionarios_evaluados']:,}")
            
            # Mostrar estadísticas básicas
            stats = data.get('estadisticas', {})
            print(f"   • Aptos para ascenso: {stats.get('cumple_todos_requisitos', 0):,}")
            print(f"   • Pendientes por nivel académico: {stats.get('cumple_menos_academicos', 0):,}")
            print(f"   • Con expedientes: {stats.get('expediente_abierto', 0) + stats.get('expediente_cerrado_reciente', 0):,}")
            print()
            
            # Guardar Excel
            if data.get('archivo_excel'):
                try:
                    print("💾 Guardando archivo Excel...")
                    
                    # Decodificar el archivo base64
                    excel_content = base64.b64decode(data['archivo_excel'])
                    
                    # Crear nombre de archivo
                    fecha_str = fecha_corte.replace('-', '_')
                    filename = f"listado_ascensos_{fecha_str}.xlsx"
                    excel_path = output_dir / filename
                    
                    # Guardar archivo
                    with open(excel_path, 'wb') as f:
                        f.write(excel_content)
                    
                    print(f"✅ Archivo Excel guardado:")
                    print(f"   📄 Archivo: {excel_path.absolute()}")
                    print(f"   📏 Tamaño: {len(excel_content):,} bytes ({len(excel_content)/1024/1024:.2f} MB)")
                    
                    return True
                    
                except Exception as e:
                    print(f"❌ Error guardando Excel: {e}")
                    return False
            else:
                print("⚠️  No se generó archivo Excel en la respuesta del servidor")
                print("💡 Esto puede deberse a:")
                print("   • Error en la generación del Excel en el módulo ascenso.py")
                print("   • Problema con las dependencias de openpyxl o PIL")
                print("   • Falta de permisos para acceder a las fotos")
                print()
                print("🔧 Intentando usar el endpoint directo de exportación...")
                
                # Intentar usar el endpoint directo de exportación
                try:
                    export_url = f"{ETL_SERVER_URL}/api/ascenso/exportar-excel/{fecha_corte}"
                    print(f"📡 Solicitando: {export_url}")
                    
                    export_response = requests.get(export_url, timeout=300)
                    
                    if export_response.status_code == 200:
                        # Crear nombre de archivo
                        fecha_str = fecha_corte.replace('-', '_')
                        filename = f"listado_ascensos_{fecha_str}.xlsx"
                        excel_path = output_dir / filename
                        
                        # Guardar archivo directamente
                        with open(excel_path, 'wb') as f:
                            f.write(export_response.content)
                        
                        print(f"✅ Archivo Excel guardado (método alternativo):")
                        print(f"   📄 Archivo: {excel_path.absolute()}")
                        print(f"   📏 Tamaño: {len(export_response.content):,} bytes ({len(export_response.content)/1024/1024:.2f} MB)")
                        
                        return True
                    else:
                        print(f"❌ Error en endpoint alternativo: {export_response.status_code}")
                        return False
                        
                except Exception as e:
                    print(f"❌ Error en método alternativo: {e}")
                    return False
                
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
    print("📋 GENERADOR DE EXCEL DE ASCENSOS")
    print("=" * 40)
    
    # Obtener fecha
    if len(sys.argv) > 1:
        fecha = sys.argv[1]
        try:
            # Validar formato de fecha
            datetime.strptime(fecha, '%Y-%m-%d')
        except ValueError:
            print("❌ Formato de fecha inválido. Usa: YYYY-MM-DD")
            print("Ejemplo: python generar_excel_ascenso.py 2024-12-31")
            sys.exit(1)
    else:
        # Usar fecha actual
        fecha = date.today().strftime('%Y-%m-%d')
        print(f"📅 Usando fecha actual: {fecha}")
    
    # Obtener directorio de salida (opcional)
    directorio_salida = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Generar Excel
    success = generar_excel_ascenso(fecha, directorio_salida)
    
    if success:
        print("\n🎉 ¡Excel generado exitosamente!")
        print("\n💡 El archivo Excel contiene:")
        print("   • Listado completo de funcionarios")
        print("   • Fotos de los funcionarios")
        print("   • Columnas para marcar 'Apto' y 'No Apto'")
        print("   • Información completa para evaluación")
    else:
        print("\n💥 Error generando el Excel")
        sys.exit(1)

if __name__ == "__main__":
    main()
