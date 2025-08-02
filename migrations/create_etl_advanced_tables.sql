-- Tabla para versiones de configuraciones ETL
CREATE TABLE IF NOT EXISTS etl_config_versions (
    id SERIAL PRIMARY KEY,
    config_id INTEGER REFERENCES etl_configs(id) ON DELETE CASCADE,
    version VARCHAR(20) NOT NULL,
    config_data JSONB NOT NULL,
    is_active BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT etl_config_versions_unique UNIQUE (config_id, version)
);

-- Tabla para historial de cargas ETL
CREATE TABLE IF NOT EXISTS etl_load_history (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    config_name VARCHAR(255),
    source_file VARCHAR(500) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    mode VARCHAR(50) NOT NULL,
    total_rows INTEGER DEFAULT 0,
    inserted_rows INTEGER DEFAULT 0,
    updated_rows INTEGER DEFAULT 0,
    error_rows INTEGER DEFAULT 0,
    success_rate DECIMAL(5,2) DEFAULT 0,
    execution_time INTEGER, -- en segundos
    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed
    error_message TEXT,
    rollback_data JSONB, -- datos para rollback
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Tabla para validaciones de datos
CREATE TABLE IF NOT EXISTS etl_data_validations (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    validation_type VARCHAR(100) NOT NULL, -- null_count, duplicate_count, type_mismatch, etc.
    validation_result JSONB NOT NULL,
    severity VARCHAR(20) DEFAULT 'info', -- info, warning, error
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para transformaciones personalizadas
CREATE TABLE IF NOT EXISTS etl_custom_transformations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    python_code TEXT NOT NULL,
    parameters JSONB, -- parámetros esperados
    category VARCHAR(100) DEFAULT 'custom',
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para configuración de notificaciones
CREATE TABLE IF NOT EXISTS etl_notification_configs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(50) NOT NULL, -- email, slack, telegram, webhook
    config JSONB NOT NULL, -- configuración específica del tipo
    events JSONB NOT NULL, -- eventos que disparan la notificación
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para logs de notificaciones
CREATE TABLE IF NOT EXISTS etl_notification_logs (
    id SERIAL PRIMARY KEY,
    config_id INTEGER REFERENCES etl_notification_configs(id),
    load_history_id INTEGER REFERENCES etl_load_history(id),
    event_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL, -- sent, failed, pending
    message TEXT,
    error_message TEXT,
    sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para trabajos en cola
CREATE TABLE IF NOT EXISTS etl_job_queue (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    job_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed
    priority INTEGER DEFAULT 0,
    parameters JSONB NOT NULL,
    progress INTEGER DEFAULT 0,
    result JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Índices para optimización
CREATE INDEX IF NOT EXISTS idx_etl_config_versions_config_id ON etl_config_versions(config_id);
CREATE INDEX IF NOT EXISTS idx_etl_config_versions_active ON etl_config_versions(config_id, is_active);
CREATE INDEX IF NOT EXISTS idx_etl_load_history_session ON etl_load_history(session_id);
CREATE INDEX IF NOT EXISTS idx_etl_load_history_status ON etl_load_history(status);
CREATE INDEX IF NOT EXISTS idx_etl_load_history_created ON etl_load_history(created_at);
CREATE INDEX IF NOT EXISTS idx_etl_data_validations_session ON etl_data_validations(session_id);
CREATE INDEX IF NOT EXISTS idx_etl_job_queue_status ON etl_job_queue(status);
CREATE INDEX IF NOT EXISTS idx_etl_job_queue_priority ON etl_job_queue(priority DESC);

-- Comentarios
COMMENT ON TABLE etl_config_versions IS 'Versiones de configuraciones ETL';
COMMENT ON TABLE etl_load_history IS 'Historial de cargas ETL con capacidad de rollback';
COMMENT ON TABLE etl_data_validations IS 'Resultados de validaciones de calidad de datos';
COMMENT ON TABLE etl_custom_transformations IS 'Transformaciones personalizadas en Python';
COMMENT ON TABLE etl_notification_configs IS 'Configuraciones de notificaciones';
COMMENT ON TABLE etl_notification_logs IS 'Log de notificaciones enviadas';
COMMENT ON TABLE etl_job_queue IS 'Cola de trabajos ETL para procesamiento asíncrono';
