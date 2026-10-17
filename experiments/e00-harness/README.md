# E00. Humo del arnés

Comprueba el runner, `expctl` y el reporte sin servicios de negocio.

```sh
mise run exp:e00 && mise run exp:report
```

- `drive`: 30 s de `expctl sample` (cada 2 s) con las réplicas de `kafka-exporter` y `up{job="prometheus"}`.
- `collect`: consulta instantánea y de rango a Prometheus (`prom/`) y `metrics.json`.
- `criteria.yaml`: 4 criterios (muestras, réplicas listas, `up` = 1, puntos del rango).

Resultado en `experiments/results/e00-harness/latest/`.
