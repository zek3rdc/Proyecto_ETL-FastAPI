import os
from pathlib import Path

# Configuración de base de datos
DATABASE_CONFIG = {
    "host": "localhost",
    "database": "jupe",
    "user": "postgres",
    "password": "12345678",  # Cambiar por la contraseña real
    "port": 5432
}

# Configuración de la aplicación
APP_CONFIG = {
    "host": "0.0.0.0",
    "port": 8001,
    "debug": True,
    "reload": True
}

# Directorio para archivos temporales
UPLOAD_DIR = Path(__file__).parent / "temp_uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# Configuración de logging
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO"
        },
        "file": {
            "class": "logging.FileHandler",
            "filename": "logs/etl_app.log",
            "formatter": "default",
            "level": "INFO"
        }
    },
    "root": {
        "handlers": ["console", "file"],
        "level": "INFO"
    }
}

# Crear directorio de logs si no existe
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)
