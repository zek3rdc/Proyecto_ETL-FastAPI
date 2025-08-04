# 📋 Generador de Listado de Ascensos

Este conjunto de scripts te permite generar listados de ascensos utilizando el módulo `ascenso.py` del sistema ETL.

## 🚀 Inicio Rápido

### Opción 1: Solo generar Excel (RECOMENDADO)
```bash
# Para fecha actual
python generar_excel_ascenso.py

# Para fecha específica
python generar_excel_ascenso.py 2024-12-31

# Con directorio específico
python generar_excel_ascenso.py 2024-12-31 C:\MisArchivos
```

### Opción 2: Usar el archivo batch (Windows)
```bash
# Ejecutar el generador interactivo
generar_ascenso.bat
```

### Opción 3: Script simple de Python (con JSON)
```bash
# Para fecha actual
python generar_ascenso_simple.py

# Para fecha específica
python generar_ascenso_simple.py 2024-12-31
```

### Opción 4: Script completo con opciones
```bash
# Ejemplo básico
python generar_listado_ascenso.py --fecha 2024-12-31

# Con opciones avanzadas
python generar_listado_ascenso.py --fecha 2024-12-31 --incluir-inactivos --no-excel
```

## 📁 Archivos Incluidos

### 1. `generar_excel_ascenso.py` ⭐ RECOMENDADO
- **Propósito**: Genera SOLO el archivo Excel de ascensos
- **Uso**:
  ```bash
  python generar_excel_ascenso.py [fecha] [directorio_salida]
  ```
- **Características**:
  - Enfocado únicamente en generar el Excel
  - No genera archivos JSON innecesarios
  - Muestra estadísticas básicas
  - Más rápido y eficiente

### 2. `generar_ascenso.bat`
- **Propósito**: Interfaz interactiva para Windows
- **Características**:
  - Verifica automáticamente si el servidor ETL está ejecutándose
  - Ofrece opciones para iniciar el servidor si no está activo
  - Menú interactivo con opción de "Solo Excel"
  - Manejo de errores amigable

### 3. `generar_ascenso_simple.py`
- **Propósito**: Script Python simple y directo
- **Uso**:
  ```bash
  python generar_ascenso_simple.py [fecha_opcional]
  ```
- **Características**:
  - Genera listado para fecha actual si no se especifica fecha
  - Configuración predeterminada (solo activos y uniformados)
  - Genera tanto Excel como JSON

### 4. `generar_listado_ascenso.py`
- **Propósito**: Script completo con todas las opciones
- **Uso**:
  ```bash
  python generar_listado_ascenso.py --fecha YYYY-MM-DD [opciones]
  ```
- **Opciones disponibles**:
  - `--fecha`: Fecha de corte (requerida)
  - `--incluir-inactivos`: Incluir funcionarios inactivos
  - `--incluir-no-uniformados`: Incluir funcionarios no uniformados
  - `--no-excel`: No generar archivo Excel
  - `--directorio-salida`: Especificar directorio de salida

## 🔧 Requisitos Previos

### 1. Servidor ETL Activo
El servidor ETL debe estar ejecutándose en `localhost:8001`. Para iniciarlo:

```bash
# Opción 1: Usar el batch
start_etl.bat

# Opción 2: Ejecutar directamente
cd etl_app
python main.py
```

### 2. Dependencias Python
Asegúrate de tener instaladas las dependencias:
```bash
pip install requests
```

### 3. Base de Datos
- PostgreSQL debe estar ejecutándose
- Base de datos `jupe` debe estar accesible
- Configuración en `config.py` debe ser correcta

## 📊 Salida del Script

### Archivos Generados

1. **Archivo Excel**: `listado_ascensos_YYYY_MM_DD.xlsx`
   - Contiene el listado completo con fotos
   - Columnas para marcar "Apto" y "No Apto"
   - Formato listo para evaluación

2. **Archivo JSON**: `listado_ascensos_YYYY_MM_DD.json`
   - Datos completos en formato JSON
   - Útil para procesamiento adicional
   - Incluye todas las estadísticas

### Información Mostrada

- **Estadísticas generales**: Total de funcionarios evaluados
- **Distribución por categorías**:
  - Cumple todos los requisitos
  - Cumple menos académicos
  - Expediente cerrado reciente
  - Expediente abierto
  - Condición actual inválida
- **Ejemplos de funcionarios** por cada categoría

## 🎯 Ejemplos de Uso

### Caso 1: Solo Excel para fin de año (RECOMENDADO)
```bash
python generar_excel_ascenso.py 2024-12-31
```

### Caso 2: Solo Excel en directorio específico
```bash
python generar_excel_ascenso.py 2024-12-31 C:\Ascensos2024
```

### Caso 3: Solo Excel para fecha actual
```bash
python generar_excel_ascenso.py
```

### Caso 4: Listado completo incluyendo inactivos
```bash
python generar_listado_ascenso.py --fecha 2024-12-31 --incluir-inactivos
```

### Caso 5: Solo datos JSON, sin Excel
```bash
python generar_listado_ascenso.py --fecha 2024-12-31 --no-excel
```

## 🔍 Solución de Problemas

### Error: "Servidor ETL no está disponible"
**Solución**: 
1. Ejecuta `start_etl.bat` para iniciar el servidor
2. Espera unos segundos a que inicie completamente
3. Vuelve a ejecutar el script

### Error: "Error de conexión a la base de datos"
**Solución**:
1. Verifica que PostgreSQL esté ejecutándose
2. Revisa la configuración en `config.py`
3. Asegúrate de que la base de datos `jupe` exista

### Error: "Timeout en la solicitud"
**Solución**:
- Esto es normal con bases de datos grandes
- El script tiene un timeout de 5 minutos
- Si persiste, verifica la conexión a la base de datos

### Error: "No se puede guardar el archivo Excel"
**Solución**:
1. Verifica que no tengas el archivo abierto en Excel
2. Asegúrate de tener permisos de escritura en la carpeta
3. Usa la opción `--no-excel` si solo necesitas los datos JSON

## 📋 Parámetros del Endpoint

El script utiliza el endpoint `/api/ascenso/generar-listado` con estos parámetros:

```json
{
  "fecha_corte": "2024-12-31",
  "incluir_solo_activos": true,
  "incluir_solo_uniformados": true
}
```

## 🔄 Flujo del Proceso

1. **Verificación**: Comprueba que el servidor ETL esté activo
2. **Solicitud**: Envía petición POST al endpoint de ascenso
3. **Procesamiento**: El servidor procesa todos los funcionarios
4. **Respuesta**: Recibe datos con estadísticas y listas organizadas
5. **Guardado**: Genera archivos Excel y JSON
6. **Reporte**: Muestra estadísticas en consola

## 📞 Soporte

Si encuentras problemas:
1. Revisa los logs en `etl_app/logs/etl_app.log`
2. Verifica la configuración de base de datos
3. Asegúrate de que todas las dependencias estén instaladas
4. Consulta este README para soluciones comunes

---

**Nota**: Estos scripts están diseñados para trabajar con el módulo `ascenso.py` existente sin modificarlo, utilizando sus endpoints REST para generar los listados de manera eficiente.
