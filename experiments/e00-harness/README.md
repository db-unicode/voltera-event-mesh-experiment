# E00. Humo del arnés

Comprueba el runner, `expctl` y el reporte sin servicios de negocio.

```sh
mise run exp:e00 && mise run exp:report
```

- `drive`: 30 s de `expctl sample` (cada 2 s) con las réplicas de `kafka-exporter` y `up{job="prometheus"}`.
- `collect`: consulta instantánea y de rango a Prometheus (`prom/`) y `metrics.json`.
- `criteria.yaml`: 4 criterios (muestras, réplicas listas, `up` = 1, puntos del rango).

## Resultado

Corrida final `20261017T064459Z-18789b0-r1` (`experiments/results/final/e00-harness/`, ronda `final-run-18789b0.tsv`): 4/4 criterios en PASS.

- `samples-taken`: 15 muestras (>= 10).
- `kafka-exporter-ready`: 1 (>= 1).
- `prometheus-up`: 1 (= 1).
- `prom-range-points`: 16 puntos (>= 10).
