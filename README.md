# Laboratorio de agregaciones JSON bajo contención

Estado de esta entrega: fases 00–08 completadas; la fase 09 está pausada en el checkpoint de la campaña extendida. Tiene 41/162 celdas terminales y 44 intentos. La celda 41 terminó con timeout de PostgreSQL durante warmup, sin ventana medida; se corrigió su clasificación como resultado terminal del motor. No hay runner activo. Consulta `docs/estado-actual.md` y `docs/comando-benchmark-09.md` para reanudar.

1. Descomprime esta carpeta en el equipo Linux donde realizarás las mediciones.
2. Abre `benchmark-seguridad/` como raíz de trabajo de Codex, con acceso a su terminal local. Una sesión remota sin acceso al Docker de tu equipo no puede medir ese host.
3. Lee `PLAN_CODEX.md` y pega primero `prompts/00_inicio_y_diagnostico.md`.
4. Continúa en orden con 01 a 08; revisa sus criterios antes de avanzar.
5. La campaña extendida se prepara con `./run_benchmarks.sh --mode extended`; prompt 09 es el paso de ejecución largo. Puede dejarse activo en Terminal persistente y no requiere esperar en Codex.
6. Termina con 10 para auditar y empaquetar el laboratorio.

`PLAN_CODEX.md` contiene todos los prompts completos; `prompts/` permite abrirlos individualmente. `AGENTS.md` conserva las reglas cuando cambie la sesión. Usa `prompts/90_retomar_sesion.md` para continuar y `91_resolver_fallo.md` para corregir un problema.

## Dominio y alcance

Eventos sintéticos de seguridad de red. Un evento contiene un dispositivo, su ubicación, un incidente y varias detecciones. Q1 desanida las detecciones, filtra por impacto y agrupa por país y categoría. Q2 obtiene los 1000 incidentes confirmados con mayor riesgo desde una fecha. Es un laboratorio local de bases de datos; el reporte académico y las gráficas interpretativas quedan pendientes.

## Incluido y pendiente

Incluido: fuentes PDF originales, alcance adaptado, modelo JSON, contratos, perfiles, matriz declarativa, consultas e índices de referencia, JSONL oficial, importación de ambos motores, equivalencia Q1/Q2 con fixture, planes reales, telemetría, runner concurrente, orquestación, dry-run/checkpoint y auditoría de pilotos.
Pendiente: ejecutar la matriz extendida con prompt 09, validar su salida y cerrar el paquete con prompt 10; después queda el reporte académico. Los cuatro pilotos son evidencia de fase 08, no resultados oficiales de la matriz.

Los PDF son fuentes de la actividad. Las decisiones del dominio y los contratos adaptados están en `context/` y `contracts/`. Conserva evidencia de cualquier desviación técnica respecto al protocolo docente.

## Recorrido de carpetas

| Ruta | Función |
|---|---|
| `context/` | Alcance, modelo y equivalencia de consultas |
| `contracts/` | Datos, métricas, ciclo de vida y CLI esperada |
| `config/` | Perfiles, matriz y futura infraestructura de observabilidad |
| `queries/` | Consultas e índices de ambos motores |
| `scripts/` | Generador, validador, preflight, carga, equivalencia y planes implementados; runner y automatización pendientes |
| `benchmark_runner/` | Runner por celda implementado; smoke/concurrencia real pendientes |
| `telemetry/` | Colector Prometheus y evidencia de spill/OOM implementados; no integrados aún al runner |
| `tests/` | Pruebas del generador/validador implementadas; no hay pruebas de rendimiento |
| `data/` | Dataset sintético oficial JSONL de 500 000 eventos y manifiesto SHA-256 |
| `results/` | CSV, manifiestos, muestras, planes y logs reales |
| `docs/fuentes/` | PDF originales |
| `docs/` | Diagnóstico, decisiones, operación y avance |

Las versiones de motores y dependencias se resuelven en el equipo durante la implementación y después se fijan. No se utiliza `latest` como versión reproducible.
