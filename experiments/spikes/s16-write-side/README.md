# S16. Lado de escritura en r1

Verifica de punta a punta `meter-sim -> meter-declarations -> declaration-gateway -> positions-db + outbox -> energy-positions ->
market-projector -> Redis` y el escalado KEDA del gateway. También es la primera corrida real del generador de carga (S11).

```sh
mise run brokers:rabbitmq && mise run data:up r1
mise run apps:write-side r1      # el spike lo vuelve a aplicar; SKIP_DEPLOY=1 lo omite
mise run spike:s16               # C_POD=46 SPIKE_S=60 por defecto
```

Evidencia en `experiments/results/s16/<UTC>-<sha>/`: `result.txt`, `checks/N.log` (salida de cada check), `once-summary.json`,
`dup-summary.json`, `spike-summary.json` (resúmenes de meter-sim), `samples.csv` (`ts,replicas,depth` cada 2 s),
`prom-p95-*.json`, `lag-p95-*.txt`, `ttls.txt`, `market-projector` (binario) y `projector-modules.txt`, `top-pods.txt`, `scaledobject.yaml`.

| Check | Cómo |
|---|---|
| (a) alta | `once --meters 200 --prefix s16-` (100 acme-v1 + 100 voltix-2) => 200 filas en `positions`, 200 keys `pos:*:s16-*` (50 por zona), TTL 100-150 s, campos del contrato, `declarations_processed_total` +200, outbox sin pendientes |
| (b) duplicados | `dup --dup-k 1` con la misma semilla (mismo lote): `events_stale_dropped_total` +200; `positions` (versión y `updated_at`), outbox y Redis (key, `_v`) idénticos |
| (c) formato desconocido | `once --meters 1 --vendor mystery-v3`: `declarations_rejected_total{reason="unknown_vendor"}` +1, `events_consumed_total{result="error"}` +1, no se reencola, nada en positions-db ni Redis |
| (d) precio | `price:r1` presente; su `_v` cambia en <= 45 s |
| (e) KEDA | en reposo 0 réplicas; `spike` de 6 x C_pod (276 msg/s) 60 s con rampa de 5 s sobre 500 medidores; réplicas >= 2 (<= 10) y vuelta a 0 con cola 0; después 500 posiciones, 500 keys y outbox sin pendientes |
| (f) pgx | `go build ./cmd/market-projector` y `go version -m` / `go list -deps` sin `jackc/pgx` |
| (g) latencia | `histogram_quantile(0.95, increase(event_e2e_lag_seconds_bucket{service="market-projector",stage="committed"}[10m]))` en Prometheus, tras el lote de (a) y tras el spike; límite 5 s |

Los checks (a)-(d) fijan el gateway en 1 réplica con la annotation `autoscaling.keda.sh/paused-replicas=1` para leer sus contadores
por port-forward (los pods que KEDA apaga se llevan los suyos); el check (e) la quita. El `trap` la quita siempre.
Los datos del spike llevan prefijo `s16`/`s16e-`/`s16bad-` y se borran al empezar y al pasar; no toca otras keys de `market-view`.

## Resultados (dos corridas PASS)

- (a) 200 posiciones y 200 keys (50 por zona); TTL medido 142-149 s a los pocos segundos de escribirlas.
- (b) `events_stale_dropped_total` 0 -> 200; `positions`, outbox y Redis idénticos.
- (e) con `value: 100`, `C_pod = 46` y 276 msg/s durante 60 s: 0 réplicas, pico de 10 (el máximo) con 6327-6805 mensajes en cola,
  y vuelta a 0 réplicas con cola 0 (la baja a 0 la hace KEDA tras `cooldownPeriod`; las 10 réplicas se mantienen unos 20 s tras vaciarse la cola).
  15870 declaraciones publicadas, 0 errores, 0 descartadas, 24 `late` (> 100 ms de retraso, máx. 151 ms).
- (g) committed -> vista: p50 ~7 ms, p95 ~24 ms (el histograma interpola dentro del bucket 10-25 ms). Las ventanas de 10 min
  incluyen corridas anteriores, por eso el recuento es mayor que el lote.
