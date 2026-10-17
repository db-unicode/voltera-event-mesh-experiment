# S13. Métricas Go en Prometheus

`internal/telemetry` expone `/metrics` (client_golang) con las métricas comunes de la tabla 4.2 y una etiqueta constante `service`.

## Ejecutar

```sh
mise run spike:s13
```

## Montaje

- `cmd/metrics-probe`: servicio sintético que consume un evento cada 200 ms (ok / duplicate / stale) y sirve `/metrics`, `/healthz`, `/readyz`, `/debug/state` en `:9090`.
- Anotaciones `prometheus.io/scrape|port|path` en el pod. Con sidecar, Istio las reescribe a `:15020/stats/prometheus` y fusiona las métricas de la app con las de Envoy (`prometheus.istio.io/merge-metrics` por defecto). El Prometheus de `istio-system` (job `kubernetes-pods`) solo lee esas anotaciones.
- Un pod `curl` llama a `metrics-probe:9090/healthz` cada segundo para generar `istio_requests_total`.

## Checks

Anotación reescrita a 15020; target `up` con `scrapeUrl` en `:15020/stats/prometheus`; en Prometheus: `events_consumed_total{service="metrics-probe"}` (ok, duplicate, stale), `event_e2e_lag_seconds`, `go_goroutines`, `istio_requests_total` del mismo servicio, y que el contador crece en 20 s.

Prometheus raspa cada 15 s, así que el spike espera hasta 2 min por las primeras series.

## Evidencia

`experiments/results/s13/<UTC>-<sha>/`: `result.txt`, `pod.json`, `targets.json`, `events_consumed_total.json`, `istio_requests_total.json`, `top-pods.txt`, `checks/`.
