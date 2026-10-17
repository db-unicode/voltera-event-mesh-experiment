# E1: consumidor caído, el emisor no se entera

```sh
mise run exp:run e01      # o: mise run exp:run e01 <rep>
```

Dura ~10 min (dos fases de ~4 min). Toma el lock del cluster. Deja réplicas, KEDA y `market-projector` como estaban.

## Hipótesis

Si un consumidor se cae, el emisor sigue operando con la misma latencia y sin errores. Los eventos esperan en el
broker (topic con log retenido en Kafka, cola durable en RabbitMQ) y se procesan al volver sin perder ninguno ni
duplicar efectos.

## Variables

| Variable | Valor |
|---|---|
| Consumidor caído | a: `energy-transfer` (KEDA pausado en 0: `paused-replicas=0`); b: `market-projector` (`scale --replicas=0`) |
| Duración de la caída | 60 s (< `VIEW_TTL_S` = 150 s) |
| Carga | `meter-sim --profile pairs`, 20 pares/s (40 declaraciones/s) durante 180 s, `--horizon 30s`, región r1 |
| Medidores | a: 1200 (vuelta de 30 s); b: 400 (vuelta de 10 s, todos en la vista antes de la caída) |
| Instante de la caída | 30 s después del primer commit de la carga en `positions-db` |
| Prefijo | `e01a-<epoch>-` / `e01b-<epoch>-`: todos los conteos se hacen por prefijo |

## Procedimiento

1. `exp_reset`: quita pausas de KEDA, abre port-forwards (19800, 19801, 19803, 19804) y espera reposo (lag 0, colas 0, outbox vacías).
2. Fase a: arranca meter-sim (Job) y los muestreadores. A t=30 s `chaos_pause_keda energy-transfer 0`; a t=90 s `chaos_resume_keda`. Espera a que termine la carga y a que todo drene (lag 0, outbox vacías, una transferencia `SENT` por match, efecto en device-sim y billing-sim).
3. Fase b: igual, con `market-projector` a 0 réplicas y 1 réplica fija del gateway (`paused-replicas=1`), para que su p95 no dependa del cold start de KEDA.
4. `collect`: calcula las métricas por prefijo y escribe `metrics.json`; `expctl assert` evalúa `criteria.yaml`.

## Medición

| Qué | Cómo |
|---|---|
| Pérdida y duplicados | `matchId` de `matches` (`matches-db`) con el prefijo, comparados uno a uno con `device-sim /state` y `billing-sim /state?detail=1`: ausentes = pérdida, `count > 1` = efecto duplicado |
| Delta | matches creados entre inicio y asentamiento de la fase menos efectos distintos nuevos (`distinct` antes/después) en device-sim y billing-sim |
| Emisor a (matching-engine) | p95 de `match_window_seconds` (duración de la ventana de emparejamiento) en los 30 s previos vs los 60 s de caída; matches/s; `outbox_publish_total{result="error"}` en toda la fase; máximo de `outbox_pending` |
| Emisor b (declaration-gateway) | igual con `event_processing_seconds` y `declarations_processed_total` del gateway |
| Lag / cola | `kafka_consumergroup_lag_sum{consumergroup="energy-transfer",topic="r1.energy-matched"}` leído del kafka-exporter en cada muestra (~1,5 s); cola `energy-positions` con `rabbitmqctl list_queues` (exacta; el management API refresca cada 5 s). `raw/series-a.csv` y `raw/series-b.csv` |
| Crecimiento | (valor al reanudar) − (valor al caer) frente a los eventos producidos entre ambos instantes (matches en `matches-db` / filas de outbox de `positions-db`) |
| Drenado | desde la primera muestra con el consumidor Ready tras reanudar hasta la primera con lag/cola 0 |
| Vista (b) | `redis-cli --scan --pattern 'pos:*:<prefijo>*'` cada ~3 s (`raw/keys-b.csv`): keys antes de caer, mínimo hasta el fin de la carga, y al fin de la carga |

Series Prometheus: `expctl prom query --url` contra el port-forward propio (consulta instantánea en el instante de interés con `increase(...[ventana])`).

## Criterios (`criteria.yaml`)

Los de 03-experiments.md 5/E1, con dos precisiones:

- "Lag crece ~ tasa x D (+/- 10 %)": se mide el crecimiento entre el instante de caer y el de reanudar (no el pico, que ocurre
  unos segundos después de reanudar porque el consumidor tarda en arrancar y el exporter se lee cada ~1,5 s).
- La línea base del p95 es la ventana de 30 s previa a la caída en la misma corrida (no una corrida aparte).

## Variante `TRANSFER_MODE=sync` (no implementada)

Con acoplamiento síncrono matching-engine llamaría por HTTP a `energy-transfer`: con éste caído cada llamada fallaría o
agotaría su timeout, así que `matches` perdería eventos o la ventana de matching se bloquearía (errores > 0 y p95 muy por encima de la línea base).
Se omitió porque ese modo no existe en el código de los servicios y el experimento no puede modificarlos; la evidencia
contraria (el emisor intacto) es la que mide E1.

## Evidencia

`experiments/results/e01-consumer-down/<UTC>-<sha>-r<rep>/`: `metrics.json`, `result.json`, `raw/series-{a,b}.csv`, `raw/keys-b.csv`, `raw/phase-*.json`, `raw/effects-*.json`, `raw/ids-*.json`, `raw/device-*.json`, `raw/billing-*.json`, `figures/e01a-lag-replicas.png`, `figures/e01b-lag-replicas.png`, `logs/`.

## Resultado

RESULTADOS_PENDIENTES
