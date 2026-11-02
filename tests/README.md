# Pruebas disponibles

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

Cubren determinismo por semilla, cambio de salida entre semillas, protección ante sobrescritura y rechazo de UUID, fecha, unicidad y manifiesto inválidos. Las pruebas de equivalencia de motor, empates Top-K, ventanas/percentiles, contadores y telemetría quedan para fases posteriores. Esta suite no necesita motores ni Docker.
