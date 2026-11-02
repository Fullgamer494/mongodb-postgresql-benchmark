# Contrato de medición

## Unidad y ciclo

Una corrida identifica motor, perfil, consulta/tratamiento, concurrencia y repetición. Añadir run_id único en el nombre de carpeta y manifiesto, sin cambiar el CSV. Un motor activo a la vez; telemetría y runner fuera de su cgroup limitado. El runner debe tener recursos suficientes y demostrar que no es el cuello de botella.

Warmup 60 s; medición 300 s; cooldown 60 s. Clientes cerrados: cada cliente mantiene como máximo una consulta activa y lanza la siguiente al terminar. Crear/calentar conexiones antes de medir, barrera para iniciar el intervalo y pool con capacidad suficiente para 50 clientes. Consumir cursor/resultados completos. Medir con reloj monotónico; epoch solo identifica tiempo civil.

Definir admisión, fin y drenaje: abrir admisión durante 300 s; contabilizar completadas dentro de ese intervalo; registrar aparte pendientes, completadas durante drenaje, fallidas, canceladas y timeouts. No incorporar latencias del warmup ni del drenaje a percentiles oficiales. Registrar sesgo posible por operaciones lentas que cruzan límites, cantidades y política; no cambiar esta regla entre motores. Usar timeout y drenaje acotados declarados por configuración, diferenciando timeout de OOM. QPS = éxitos completos en ventana / duración real de ventana. Registrar duración efectiva.

## Campos del CSV

| Campo | Definición |
|---|---|
| timestamp_epoch | Inicio de medición en segundos Unix UTC |
| engine | mongodb o postgresql |
| profile | Identificador PROFILE_A_BASELINE, PROFILE_B_MEDIUM o PROFILE_C_SEVERE |
| query_id | Q1, Q2 o Q2_ALT; tratamientos resueltos con motor y manifiesto |
| concurrency | 1, 10 o 50 |
| run_number | 1, 2 o 3 |
| throughput_qps | Consultas completas exitosas en la ventana / segundos efectivos |
| latency_p50_ms / p95 / p99 | Percentiles del mismo conjunto de éxitos en ventana; método fijado y documentado |
| cpu_throttled_seconds | Diferencia del contador de throttling entre inicio y fin de medición; detectar reinicios |
| max_rss_mb | Máximo de RSS de cAdvisor en ventana; dividir bytes por 1024² y documentar convención MiB pese al nombre fijo |
| disk_spill | true/false respaldado por evidencia; vacío si desconocido |
| status | ok, spill, oom o error |

Precedencia de estados: oom > error > spill > ok. Conservar cantidades de errores parciales aun cuando haya éxitos. Si no hay éxitos, percentiles vacíos, QPS 0 solo si el intervalo fue válido; si no se midió, campo vacío. Un fallo temprano conserva duración parcial y no se mezcla con corridas completas en un resumen. Falta de telemetría obligatoria => error y razón de calidad, no cero artificial.

## Telemetría

Prometheus scrape_interval=1s, cAdvisor y Node Exporter obligatorios. Capturar RSS y throttling del contenedor exacto; CPU real consumida, I/O, disco, swap y presión del host como datos auxiliares. Guardar series crudas por corrida, consultas PromQL, rango, intervalos, etiquetas/ID de contenedor y ausencia de muestras. No integrar un rate() como si fuera contador acumulado; tratar reinicios explícitamente.

Spill: MongoDB, evidencia usedDisk/estadísticas/logs de consultas medidas; PostgreSQL, archivos temporales o contadores/logs de base aislada en la ventana. Definir cobertura y costo del instrumento; mantenerlo fijo. EXPLAIN separado es evidencia complementaria de una ejecución, no demuestra spill en todas las corridas.
OOM: inspección Docker más diferencias de memory.events/oom_kill del cgroup. No inferir OOM solo por conexión cerrada, uso alto de RAM o código 137. Guardar eventos antes de reiniciar o destruir un contenedor. Un reinicio del motor invalida contadores sin continuidad.

No es seguro tratar memory.current o working_set como RSS sin documentar discrepancia. Si una métrica requerida no existe en esa versión, resolverlo y documentarlo antes de la matriz oficial.

## M3, datos auxiliares sin modificar CSV

Guardar CPU consumida en segundos y trabajo real: consultas completas, cantidad de eventos de entrada y detecciones recorridas/filtadas cuando los planes lo permitan. No confundir 1000 filas de Top-K con documentos escaneados. Si se conoce trabajo procesado, CPU por 1000 = cpu_seconds / (documentos_procesados / 1000), declarando la estimación. RSS es ocupación, no recurso acumulable: cualquier normalización debe declarar si usa tamaño del dataset o documentos recorridos; no atribuir eficiencia por documento sin denominador comprobable.

## Caché, swap y parámetros

El PDF pide sync y drop_caches en el host antes del warmup. Herramienta futura con dry-run predeterminado y habilitación explícita; dejar registro aplicado/no_aplicado/error y privilegios. Sin flush no afirmar cumplimiento exacto. Reiniciar contenedores no sustituye drop_caches. Describir cachés del SO y del motor por separado.

mem_swappiness del PDF no se traduce directamente a un controlador por contenedor en cgroups v2. Implementar y verificar prohibición de swap mediante la configuración Docker apropiada (por ejemplo memoria+swap total igual al límite de RAM), comprobando memory.swap.max=0, y registrar la equivalencia operacional. No cambiar swappiness global. Si no es posible, marcar desviación.

Fijar y guardar WiredTiger cache, shared_buffers, max_connections, hash_mem_multiplier, paralelismo PostgreSQL y timeouts. No autotunear distinto por perfil sin una decisión previa y trazable. work_mem es por operación y puede multiplicarse con concurrencia y paralelismo; los tratamientos Mongo/PostgreSQL no representan cuotas de memoria equivalentes.

## Artefactos por corrida

results/<campaign_id>/<run_id>/ con manifest.json, summary.csv, requests.jsonl (latencias/estado, usando escritura amortiguada), telemetry/, engine_logs/, host_events/ y quality.json. El manifiesto incluye checksum/semilla/tamaño del dataset, commit/hash del código, imágenes/digests, versiones, parámetros, límites efectivos, plan de campaña, caché, tiempos/ventanas, conexiones, errores, intentos y estado.

Nunca sobrescribir un intento; reintentos generan attempt_id. El resumen canónico referencia el intento elegido, conserva fallidos y distingue recuperación del motor de reparación de infraestructura.
