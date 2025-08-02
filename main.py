from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import logging.config

from config import APP_CONFIG, LOGGING_CONFIG
from routers import etl as etl_router
from modules.expedientes import router as expedientes_router
from modules.ascenso import router as ascenso_router
from modules.generar_record_disciplinario import router as record_disciplinario_router

# Configurar logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Tool API", 
    version="1.0.0",
    description="API para la herramienta de Extracción, Transformación y Carga (ETL)."
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints Raíz ---
@app.get("/")
async def root():
    """Endpoint raíz que da la bienvenida a la API."""
    return {"message": "ETL Tool API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Endpoint de salud para verificar que la API está funcionando."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# --- Incluir Routers ---
logger.info("Registrando routers...")

# Router principal de ETL
app.include_router(etl_router.router)
logger.info("Router de ETL registrado con prefijo /api/etl")

# Routers de módulos específicos
app.include_router(expedientes_router, prefix="/api")
logger.info("Router de expedientes registrado con prefijo /api")

app.include_router(ascenso_router, prefix="/api")
logger.info("Router de ascenso registrado con prefijo /api")

app.include_router(record_disciplinario_router, prefix="/api")
logger.info("Router de record disciplinario registrado con prefijo /api")

# Listar todas las rutas registradas para debug
logger.info("Rutas finales registradas en la aplicación:")
for route in app.routes:
    if hasattr(route, 'path') and hasattr(route, 'methods'):
        logger.info(f"  {list(route.methods)} {route.path}")

# --- Punto de entrada para Uvicorn ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=APP_CONFIG.get("host", "127.0.0.1"), 
        port=APP_CONFIG.get("port", 8000),
        reload=APP_CONFIG.get("reload", False)
    )
