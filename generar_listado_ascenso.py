#!/usr/bin/env python3
"""
Script para generar listado de ascensos utilizando el módulo ascenso.py
Este script hace una solicitud POST al endpoint de ascenso y genera el listado.
"""

import requests
import json
import base64
from datetime import date, datetime
import sys
import os
from pathlib import Path
import argparse


def verificar_servidor_activo():
=======
# Configuración del servidor ETL
ETL_SERVER_URL = "http://localhost:8001"
ASCENSO_ENDPOINT = f"{ETL_SERVER_URL}/api/ascenso/generar-listado"

def verificar_servidor_activo():
=======

def verificar_servidor_activo():
    """Verifica si el servidor ETL está activo"""
    try:
        response = requests.get(f"{ETL_SERVER_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Servidor ETL está activo")
            return True
        else:
            print(f"❌ Servidor ETL respondió con código: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Error conectando al servidor ETL: {e}")
        print(f"   Asegúrate de que el servidor esté ejecutándose en {ETL_SERVER_URL}")
        return False

def generar_listado_ascenso(fecha_corte, incluir_solo_activos=True, incluir_solo_uniformados=True, guardar_excel=True, directorio_salida=None):
    """
    Genera el listado de ascensos para la fecha especificada
    
    Args:
        fecha_corte (str): Fecha de corte en formato YYYY-MM-DD
        incluir_solo_activos (bool): Solo incluir funcionarios activos
        incluir_solo_uniformados (bool): Solo incluir funcionarios uniformados
        guardar_excel (bool): Guardar el archivo Excel generado
        directorio_salida (str): Directorio donde guardar los archivos (opcional)
    """
    
    # Determinar directorio de salida
    if directorio_salida:
        output_dir = Path(directorio_salida)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path.cwd()
    
    print(f"🚀 Generando listado de ascensos para fecha: {fecha_corte}")
    print(f"   - Solo activos: {incluir_solo_activos}")
    print(f"   - Solo uniformados: {incluir_solo_uniformados}")
    print(f"   - Guardar Excel: {guardar_excel}")
    print(f"   - Directorio de salida: {output_dir.absolute()}")
    print()
    
    # Preparar datos de la solicitud
    request_data = {
        "fecha_corte": fecha_corte,
        "incluir_solo_activos": incluir_solo_activos,
        "incluir_solo_uniformados": incluir_solo_uniformados
    }
    
    try:
        print("📡 Enviando solicitud al servidor ETL...")
        
        # Hacer la solicitud POST
        response = requests.post(
            ASCENSO_ENDPOINT,
            json=request_data,
            headers={"Content-Type": "application/json"},
            timeout=300  # 5 minutos de timeout para procesos largos
        )
        
        if response.status_code == 200:
            print("✅ Solicitud procesada exitosamente")
            
            # Procesar respuesta
            data = response.json()
            
            # Mostrar estadísticas
            print("\n📊 ESTADÍSTICAS DEL LISTADO:")
            print(f"   • Fecha de corte: {data['fecha_corte']}")
            print(f"   • Total funcionarios evaluados: {data['total_funcionarios_evaluados']}")
            print()
            
            estadisticas = data.get('estadisticas', {})
            print("📋 DISTRIBUCIÓN POR CATEGORÍAS:")
            print(f"   • Cumple todos los requisitos: {estadisticas.get('cumple_todos_requisitos', 0)}")
            print(f"   • Cumple menos académicos: {estadisticas.get('cumple_menos_academicos', 0)}")
            print(f"   • Expediente cerrado reciente: {estadisticas.get('expediente_cerrado_reciente', 0)}")
            print(f"   • Expediente abierto: {estadisticas.get('expediente_abierto', 0)}")
            print(f"   • Condición actual inválida: {estadisticas.get('condicion_actual_invalida', 0)}")
            print()
            
            # Mostrar algunos ejemplos de cada lista
            listas = data.get('listas', {})
            
            for categoria, funcionarios in listas.items():
                if funcionarios:
                    print(f"👥 {categoria.upper().replace('_', ' ')} ({len(funcionarios)} funcionarios):")
                    # Mostrar los primeros 3 funcionarios como ejemplo
                    for i, funcionario in enumerate(funcionarios[:3]):
                        print(f"   {i+1}. {funcionario['nombre_completo']} (C.I: {funcionario['cedula']}) - {funcionario['rango_actual']} → {funcionario['rango_a_aplicar']}")
                    if len(funcionarios) > 3:
                        print(f"   ... y {len(funcionarios) - 3} más")
                    print()
            
            # Guardar archivo Excel si está disponible y se solicita
            filename = None
            if guardar_excel and data.get('archivo_excel'):
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
                    
                    print(f"✅ Archivo Excel guardado como: {excel_path.absolute()}")
                    print(f"   Tamaño: {len(excel_content):,} bytes ({len(excel_content)/1024/1024:.2f} MB)")
                    
                except Exception as e:
                    print(f"❌ Error guardando archivo Excel: {e}")
                    filename = None
            
            # Guardar respuesta JSON completa
            json_filename = f"listado_ascensos_{fecha_corte.replace('-', '_')}.json"
            json_path = output_dir / json_filename
            with open(json_path, 'w', encoding='utf-8') as f:
                # Remover el archivo Excel del JSON para que sea más pequeño
                data_copy = data.copy()
                if 'archivo_excel' in data_copy:
                    excel_status = filename if filename else 'no guardado'
                    data_copy['archivo_excel'] = f"[Archivo Excel guardado como {excel_status}]"
                json.dump(data_copy, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"💾 Respuesta completa guardada como: {json_path.absolute()}")
            
            return True
            
        else:
            print(f"❌ Error en la solicitud: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Detalle: {error_data.get('detail', 'Error desconocido')}")
            except:
                print(f"   Respuesta: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout: La solicitud tardó demasiado en procesarse")
        print("   Esto puede ocurrir con bases de datos grandes. Intenta nuevamente.")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Error en la solicitud: {e}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(
        description="Genera listado de ascensos utilizando el módulo ascenso.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python generar_listado_ascenso.py --fecha 2024-12-31
  python generar_listado_ascenso.py --fecha 2024-12-31 --no-excel
  python generar_listado_ascenso.py --fecha 2024-12-31 --incluir-inactivos
        """
    )
    
    parser.add_argument(
        '--fecha', 
        type=str, 
        required=True,
        help='Fecha de corte para el listado (formato: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--incluir-inactivos',
        action='store_true',
        help='Incluir funcionarios inactivos (por defecto solo activos)'
    )
    
    parser.add_argument(
        '--incluir-no-uniformados',
        action='store_true',
        help='Incluir funcionarios no uniformados (por defecto solo uniformados)'
    )
    
    parser.add_argument(
        '--no-excel',
        action='store_true',
        help='No guardar el archivo Excel generado'
    )
    
    parser.add_argument(
        '--directorio-salida',
        type=str,
        help='Directorio donde guardar los archivos generados (por defecto: directorio actual)'
    )
    
    args = parser.parse_args()
    
    # Validar formato de fecha
    try:
        datetime.strptime(args.fecha, '%Y-%m-%d')
    except ValueError:
        print("❌ Error: La fecha debe estar en formato YYYY-MM-DD")
        sys.exit(1)
    
    print("🔧 GENERADOR DE LISTADO DE ASCENSOS")
    print("=" * 50)
    
    # Verificar que el servidor esté activo
    if not verificar_servidor_activo():
        print("\n💡 Para iniciar el servidor ETL, ejecuta:")
        print("   cd etl_app")
        print("   python main.py")
        print("   # o usa: start_etl.bat")
        sys.exit(1)
    
    print()
    
    # Generar listado
    success = generar_listado_ascenso(
        fecha_corte=args.fecha,
        incluir_solo_activos=not args.incluir_inactivos,
        incluir_solo_uniformados=not args.incluir_no_uniformados,
        guardar_excel=not args.no_excel,
        directorio_salida=args.directorio_salida
    )
    
    if success:
        print("\n🎉 ¡Listado generado exitosamente!")
    else:
        print("\n💥 Error generando el listado")
        sys.exit(1)

if __name__ == "__main__":
    main()
