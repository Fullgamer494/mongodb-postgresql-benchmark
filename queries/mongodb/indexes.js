// Referencia. Sustituir BASE por la base configurada al crear el cargador.
const target = db.getSiblingDB("security_benchmark");
target.events.createIndex({event_id: 1}, {name: "event_id_idx"});
target.events.createIndex({timestamp: 1}, {name: "timestamp_idx"});
// MongoDB añade su índice técnico _id. PostgreSQL usa PK id.
// No crear índices de incident.risk_score, categoría ni estado.
