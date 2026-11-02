SELECT data->'device'->'network'->>'country' AS country,
       detection->>'category' AS category,
       SUM((detection->>'occurrences')::numeric *
           (detection->>'impact_score')::numeric) AS total_impact,
       AVG((detection->>'occurrences')::numeric *
           (detection->>'impact_score')::numeric) AS avg_impact,
       COUNT(*) AS count
FROM events
CROSS JOIN LATERAL jsonb_array_elements(data->'detections') AS detection
WHERE (detection->>'impact_score')::numeric > 50
GROUP BY 1, 2;
