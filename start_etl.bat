@echo off
echo Iniciando ETL Tool...

REM Activar entorno virtual
call venv\Scripts\activate

REM Iniciar servidor FastAPI
python main.py

pause
