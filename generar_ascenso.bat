@echo off
echo ========================================
echo    GENERADOR DE LISTADO DE ASCENSOS
echo ========================================
echo.

REM Verificar si Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no está instalado o no está en el PATH
    echo Por favor instala Python 3.7 o superior
    pause
    exit /b 1
)

REM Cambiar al directorio del script
cd /d "%~dp0"

REM Verificar si el servidor ETL está ejecutándose
echo Verificando servidor ETL...
python -c "import requests; requests.get('http://localhost:8001/health', timeout=2)" >nul 2>&1
if errorlevel 1 (
    echo.
    echo ADVERTENCIA: El servidor ETL no está ejecutándose
    echo ¿Deseas iniciarlo ahora? (S/N)
    set /p respuesta=
    if /i "%respuesta%"=="S" (
        echo Iniciando servidor ETL...
        start "Servidor ETL" cmd /k "python main.py"
        echo Esperando que el servidor inicie...
        timeout /t 5 /nobreak >nul
    ) else (
        echo.
        echo Para iniciar el servidor manualmente, ejecuta:
        echo   start_etl.bat
        echo.
        pause
        exit /b 1
    )
)

echo.
echo Selecciona una opción:
echo 1. Generar SOLO EXCEL para fecha actual
echo 2. Generar SOLO EXCEL para fecha específica
echo 3. Generar listado completo para fecha actual
echo 4. Generar listado completo para fecha específica
echo 5. Generar con opciones avanzadas
echo.
set /p opcion="Ingresa tu opción (1-5): "

if "%opcion%"=="1" (
    echo.
    echo Generando SOLO EXCEL para fecha actual...
    python generar_excel_ascenso.py
) else if "%opcion%"=="2" (
    echo.
    set /p fecha="Ingresa la fecha (YYYY-MM-DD): "
    echo Generando SOLO EXCEL para fecha: %fecha%
    python generar_excel_ascenso.py %fecha%
) else if "%opcion%"=="3" (
    echo.
    echo Generando listado completo para fecha actual...
    python generar_ascenso_simple.py
) else if "%opcion%"=="4" (
    echo.
    set /p fecha="Ingresa la fecha (YYYY-MM-DD): "
    echo Generando listado completo para fecha: %fecha%
    python generar_ascenso_simple.py %fecha%
) else if "%opcion%"=="5" (
    echo.
    set /p fecha="Ingresa la fecha (YYYY-MM-DD): "
    echo.
    echo ¿Incluir funcionarios inactivos? (S/N)
    set /p inactivos=
    echo ¿Incluir funcionarios no uniformados? (S/N)
    set /p no_uniformados=
    echo ¿Generar archivo Excel? (S/N)
    set /p excel=
    
    set args=--fecha %fecha%
    if /i "%inactivos%"=="S" set args=%args% --incluir-inactivos
    if /i "%no_uniformados%"=="S" set args=%args% --incluir-no-uniformados
    if /i "%excel%"=="N" set args=%args% --no-excel
    
    echo.
    echo Ejecutando: python generar_listado_ascenso.py %args%
    python generar_listado_ascenso.py %args%
) else (
    echo Opción inválida
    pause
    exit /b 1
)

echo.
echo ========================================
echo           PROCESO COMPLETADO
echo ========================================
echo.
echo Los archivos generados están en la carpeta actual:
dir /b *.xlsx 2>nul
dir /b *.json 2>nul
echo.
pause
