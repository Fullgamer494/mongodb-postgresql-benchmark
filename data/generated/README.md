# Dataset generado

`events.jsonl` contiene los 500,000 eventos sintéticos oficiales de la semilla `20261101`. Tamaño: 349,196,514 bytes. SHA-256: `db1f045711ad2d923921f66ab09a5129cfc46f9ddca218820acee8c8007fb9f3`. `events.jsonl.manifest.json` registra ese hash, código/config/esquema, rangos y distribuciones. Validación completa ejecutada el 2026-11-01: 1,499,811 detecciones (media 2.999622/evento).

Reproducir desde la raíz del proyecto:

```bash
.venv/bin/python scripts/synthetic_data_generator.py --count 500000 --seed 20261101 --output data/generated/events.jsonl
.venv/bin/python scripts/validate_dataset.py --input data/generated/events.jsonl
```

El generador rehúsa reemplazar salidas existentes discordantes, salvo que se indique `--allow-overwrite`.
