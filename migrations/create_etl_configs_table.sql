CREATE TABLE IF NOT EXISTS etl_configs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    config_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices para mejor rendimiento
CREATE INDEX IF NOT EXISTS idx_etl_configs_name ON etl_configs(name);
CREATE INDEX IF NOT EXISTS idx_etl_configs_created_at ON etl_configs(created_at);

-- Comentarios de la tabla y columnas
COMMENT ON TABLE etl_configs IS 'Almacena configuraciones de importación ETL';
COMMENT ON COLUMN etl_configs.name IS 'Nombre único de la configuración';
COMMENT ON COLUMN etl_configs.description IS 'Descripción de la configuración';
COMMENT ON COLUMN etl_configs.config_data IS 'Configuración en formato JSON (mapeo de columnas, transformaciones, etc.)';
