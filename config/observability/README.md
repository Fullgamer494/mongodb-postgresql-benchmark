# Stack de observabilidad fijado

Prometheus usa `prometheus.yml` (scrape y evaluación de 1 s), escucha solo en `127.0.0.1:9090` y retiene 14 días en el volumen `u2lab_prometheus_data`. cAdvisor sirve internamente en `8080`, recoge contenedores Docker con cgroups v2 y housekeeping de 1 s, y habilita CPU, memoria, I/O y eventos OOM. Publica solo las etiquetas Compose `service` y `com.u2-benchmark.engine/profile`; almacena el resto de labels desactivado. Node Exporter sirve internamente en `9100`, con namespace PID del host y raíz de host montada en solo lectura para métricas de CPU/disco/swap/presión. Ninguno publica sus métricas al exterior.

Pins y digests `linux/amd64` comprobados contra manifiestos, descargados y arrancados el 2026-11-01; versiones inspeccionadas: Prometheus 3.15.1, cAdvisor 0.60.6, Node Exporter 1.12.1. Referencias en `config/images.lock.json`. cAdvisor tiene `privileged` y requiere `/`, `/var/run`, `/sys`, `/var/lib/docker`, `/dev/disk` y `/dev/kmsg`; los montajes de archivos son de lectura. La topología y necesidad de acceso privilegiado siguen el [README oficial de cAdvisor](https://github.com/google/cadvisor/blob/master/docs/running.md). Las métricas exportadas están descritas en su [documentación Prometheus](https://github.com/google/cadvisor/blob/master/docs/storage/prometheus.md).

Verificación ejecutada:

```bash
docker compose --env-file .env --env-file .env.resources config --quiet
docker compose --env-file .env --env-file .env.resources up -d --no-deps --wait prometheus cadvisor node-exporter
curl -fsS http://127.0.0.1:9090/api/v1/targets
```

Los tres targets reportaron `health=up`, sin error y `scrapeInterval=1s`. En cada ventana Q1 se validó por ID de contenedor exacto `container_memory_rss`, CPU, throttling y OOM de cAdvisor; Node Exporter entregó disco, swap y presión. La inspección muestra `NanoCpus=0` y `Memory=0` para los tres servicios de observabilidad, por lo que no heredan las cuotas 4 CPU/4 GiB del motor. Evidencia y ventanas: `results/telemetry/phase05/`.
