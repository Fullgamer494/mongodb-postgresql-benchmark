SELECT data->>'event_id' AS event_id,
       data->'device'->>'device_id' AS device_id,
       (data->'incident'->>'risk_score')::numeric AS risk_score
FROM events
WHERE data->'incident'->>'status' = 'confirmed'
  AND data->>'timestamp' >= '2026-01-01T00:00:00Z'
ORDER BY (data->'incident'->>'risk_score')::numeric DESC
LIMIT 1000;
