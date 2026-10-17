# E3. CQRS: retraso escritura -> vista

```sh
mise run exp:run e03
```

## Hipótesis

La lectura (Redis `market-view`) converge tras el commit en `positions-db` dentro de la tolerancia de negocio, también bajo
carga base y durante el pico, y no depende de `positions-db` para responder.

## Medición (caja negra, perfil `probe` de meter-sim)

- `meter-sim --profile probe`: medidores `probe-e03-<ts>-<fase>-NNNN` (50, uno por segundo, en rotación). Cada sondeo
  declara una versión que no se repite; cuando RabbitMQ confirma la publicación, lee `_v` de `pos:<zona>:<medidor>` en Redis
  cada 20 ms hasta ver esa versión y anota `visible_at` (resolución: 20 ms + RTT de Redis, ~1 ms).
- `committedAt` = `outbox.created_at` (`clock_timestamp()` dentro de la transacción), leído por SQL
  (`select event_id, ... from outbox where aggregate_id like 'probe-e03-<ts>-%'`) y unido por `eventId` con el CSV del sondeo.
  Un solo host: no hay sesgo de reloj. Un sondeo que no se ve en su `--probe-timeout` entra en los percentiles con ese valor
  (cota inferior), no se descarta.
- Credenciales: el perfil `probe` usa la contraseña del Secret `market-view-credentials` porque Redis no tiene usuarios de
  solo lectura; solo ejecuta `HGET`, `HGETALL` y `SCAN`. Es una herramienta de medición, no parte del dominio.
- Imagen `voltera/meter-sim:exp-e03-e04-e07` (los perfiles nuevos no están en `:dev` hasta integrar).

## Fases (región r1, ~12 min)

| Fase | Carga | Qué se mide |
|---|---|---|
| A | steady 3000 medidores / 30 s (100 decl/s), sondeo 60 s sin lecturas | p50/p95/p99 committed->visible; p95 de procesamiento del gateway |
| B | igual, 15 s después, con 200 lecturas/s de fondo (HGETALL de keys `pos:*` al azar, estilo matching) | idem; p95 de procesamiento del gateway |
| C | solo los lectores (200/s, 100 s); `positions-db` a 0 réplicas durante 30 s | errores de lectura y p95 de lectura con la base arriba y caída |
| D | spike a `SPIKE_RATE` (276/s) durante 60 s desde 0 réplicas del gateway, sondeo 170 s con lecturas | p99 durante el pico; recuperación |
| Convergencia | tras A+B y tras D | Redis == `positions-db` en `side, kwh, validUntil, version` (todos los medidores del prefijo, sondeos incluidos) |

Descomposición (medias de los histogramas internos, por ventana): `committed -> emitted` = media(`event_e2e_lag{committed}`) -
media(`{emitted}`); `emitted -> consumido` = media(`{emitted}`) - media(`event_processing_seconds` del proyector);
`consumido -> SET` = media(`event_processing_seconds` del proyector). Además p50/p95/p99 de cada etapa.

## Criterio

Ver `criteria.yaml`. Resumen: base p95 <= 2 s y p99 <= 5 s (A y B); pico p99 <= 30 s y sondeos enviados 60 s después del fin
del pico con p99 <= 5 s; con `positions-db` caída, 0 errores de lectura y p95 de lectura +-20%; la lectura concurrente sube el
p95 de procesamiento del gateway <= 20%; convergencia 100%.

Ajustes respecto a 03-experiments.md (medidas de ruido, no cambios de umbral):

- p95 de lectura: se compara la mediana de los p95 por segundo de cada ventana y se ignora una diferencia < 0,5 ms.
- p95 de escritura: el histograma `event_processing_seconds` tiene buckets de 25/50/100 ms; se ignora una diferencia < 5 ms.
- El p95 de cada etapa y de la escritura sale de `histogram_quantile` sobre `increase()` de la ventana de cada fase.

## Evidencia

`data/probe_{a,b,p}.csv` (sondeos), `data/outbox_probe.csv`, `raw/latency_*.csv` (una fila por sondeo con `committed_to_visible_s`
y su gráfico temporal en `figures/`), `figures/cdf_*.png`, `raw/reads_*.csv` (lecturas por segundo),
`latency_quantiles.json`, `decomposition.json`, `convergence.json`, `metrics.json`, `result.json`.
