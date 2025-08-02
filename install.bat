@echo off
echo Instalando ETL App con módulo de Expedientes...

REM Crear entorno virtual si no existe
if not exist "venv" (
    echo Creando entorno virtual...
    python -m venv venv
)

REM Activar entorno virtual
call venv\Scripts\activate

REM Instalar dependencias
echo Instalando dependencias...
pip install -r requirements.txt

REM Crear directorios necesarios
if not exist "logs" mkdir logs
if not exist "temp_uploads" mkdir temp_uploads

REM Crear archivo .env si no existe
if not exist ".env" (
    echo Creando archivo .env...
    copy .env.example .env
)

REM Crear tablas de configuración ETL
echo Creando tablas de configuración...
psql -U postgres -d jupe -f migrations/create_etl_configs_table.sql

echo.
echo Instalación completada.
echo Para iniciar la aplicación, ejecute: start_etl.bat
echo.
echo IMPORTANTE: Configure la URL de la API ETL en Laravel:
echo Agregue ETL_API_URL=http://localhost:8001 al archivo .env de Laravel
echo.

pause
