CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS events_timestamp_idx
ON events ((data->>'timestamp'));
-- El cargador debe verificar id = (data->>'event_id')::uuid.
-- No crear índice sobre incident.risk_score ni otros índices no pactados.
