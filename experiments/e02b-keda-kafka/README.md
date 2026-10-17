# E2 parte B: KEDA sobre el lag de Kafka, 0 -> N -> 0

```sh
mise run exp:run e02b     # o: mise run exp:run e02b <rep>
```

Dura ~2 min. Toma el lock del cluster. La parte A (RabbitMQ y control) es `e02-keda-rabbit`.

## Hipótesis

`energy-transfer` escala por el lag de `r1.energy-matched` hasta como mucho el número de particiones (6), procesa
los 3000 eventos una sola vez y vuelve a 0 réplicas.

## Variables

| Variable | Valor |
|---|---|
| Carga | 3000 `EnergyMatched` publicados directo al topic a 300/s (~10 s) |
| ScaledObject (sin cambios) | `minReplicaCount 0`, `maxReplicaCount 6`, `pollingInterval 5`, `cooldownPeriod 30`, `lagThreshold 50`, `limitToPartitionsWithLag` |
| Prefijo | `e02b-<epoch>`: medidores `<prefijo>-s<n>` (oferente) y `<prefijo>-b<n>` (demandante) |

Se publica directo (decisión): con `pairs` a través de meter-sim y matching-engine el motor emparejaría ~20-40 matches por
ventana de 2 s, y `energy-transfer` los consumiría a medida que llegan, sin formar un lag de 3000. Publicar directo da la ráfaga
de 3000 en 10 s que pide el plan. Los eventos son `EnergyMatched` válidos (sobre con `eventId` UUIDv7, `matchId` determinista, `traceparent`, headers `eventId`/`type`/`traceparent`).
El publicador es el nuevo subcomando `produce-matched` de `kafka-probe` (`internal/kafkaprobe/produce_matched.go`, con tests), corrido como Job con
el ServiceAccount `matching-engine` (el publicador legítimo del topic; así sigue pasando cualquier política de identidad). `exp_reset` reconstruye la imagen `voltera/kafka-probe:dev`.
Los matches inyectados también llegan a `mesh-bridge` y a billing-sim; se verifican en device-sim (efecto único) y billing-sim.

## Procedimiento

1. `exp_reset`: construye la imagen, abre port-forwards (19800, 19801, 19803, 19804) y espera `energy-transfer` en 0, lag 0 y outbox de matches vacía.
2. Muestreo: `expctl sample --interval 2s` (réplicas y lag de Prometheus) y muestreador propio de ~1,5 s con el lag leído del kafka-exporter (`raw/series.csv`).
3. A los 10 s lanza el Job `produce-matched`; espera lag 0, 3000 transferencias `SENT` y 0 réplicas (tope 300 s y 240 s).
4. `collect`: ids de `transfers` con el prefijo contra `device-sim /state` y `billing-sim /state?detail=1`.

## Criterios (`criteria.yaml`)

De 03-experiments.md 5/E2 (B): réplicas <= 6, lag final 0, vuelve a 0 réplicas en <= 90 s tras lag 0. Además: 3000 transferencias `SENT`,
sin pérdida ni duplicados en device-sim, pico de lag >= 1000 (un tercio de lo publicado: el consumo empieza antes de acabar de publicar; en 2 corridas el pico fue 1714 y 2200) y escalado real (pico de réplicas >= 2).
El lag se lee del exporter en cada muestra, no de Prometheus (scrape de 15 s), para no perder el pico de unos segundos.

## Evidencia

`experiments/results/e02b-keda-kafka/<UTC>-<sha>-r<rep>/`: `metrics.json`, `result.json`, `raw/series.csv`, `raw/replicas.csv`,
`raw/phase.json`, `raw/ids.json`, `raw/device.json`, `raw/billing.json`, `raw/keda-*.json`, `figures/e02b-lag-replicas.png`
(lag y réplicas x500), `logs/produce-matched.log`.

## Resultado

RESULTADOS_PENDIENTES
