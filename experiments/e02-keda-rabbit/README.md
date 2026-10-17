# E2 parte A: KEDA sobre RabbitMQ, pico y scale-to-zero

```sh
mise run exp:run e02      # o: mise run exp:run e02 <rep>
```

Dura ~12 min (la fase de control tarda ~6 min en drenar). Toma el lock del cluster. La parte B (Kafka) es `e02b-keda-kafka`.

## Hipótesis

`declaration-gateway` está en 0 réplicas en reposo, sube ante un pico siguiendo la profundidad de `meter-declarations`,
drena la cola sin perder mensajes y vuelve a 0. Con 1 réplica fija (KEDA pausado) el mismo pico tarda mucho más en drenar.

## Variables

| Variable | Valor |
|---|---|
| Carga | `meter-sim --profile spike`: rampa de 5 s a `SPIKE_RATE` = 276 msg/s y meseta; 60 s en total (15 870 declaraciones) |
| Medidores | 18 560 (> declaraciones): cada declaración es un medidor distinto, así que filas en `positions-db` == enviadas |
| `--horizon` | 5 s: las posiciones caducan antes de entrar en la vista y no generan matches ni tráfico en `energy-transfer` |
| ScaledObject (sin cambios) | `minReplicaCount 0`, `maxReplicaCount 10`, `pollingInterval 5`, `cooldownPeriod 30`, trigger RabbitMQ `QueueLength` con `value: 100` por réplica |
| Capacidad | `C_POD` = 46 msg/s por réplica (`PROCESSING_DELAY_MS` = 20): 10 réplicas = 460 msg/s |
| Control | `paused-replicas=1` (KEDA pausado en 1 réplica fija), misma carga |

## Procedimiento

1. `exp_reset`: quita pausas, abre port-forwards (19800, 19801, 19803, 19804) y espera gateway en 0 con la cola vacía.
2. Fase `k` (KEDA): gateway en 0 durante 30 s más; arranca muestreadores (10 s de línea base); lanza el pico; espera profundidad <= 1 y 0 réplicas (tope 660 s y 300 s).
3. Fase `c` (control): `paused-replicas=1`, espera 1 réplica lista y repite el pico. El tope de drenado son 660 s.
4. Restaura KEDA, espera gateway en 0 y que `energy-positions` se vacíe (el proyector consume lo que publicó el gateway).

Muestreo: `expctl sample --interval 2s` (réplicas deseadas/listas y profundidad según Prometheus, `raw/replicas-{k,c}.csv`) y un
muestreador propio de ~2-3 s con la profundidad exacta (`rabbitmqctl list_queues`: ready + unacked, que es lo que cuenta el
trigger de KEDA) y las réplicas (`raw/depth-{k,c}.csv`). Los criterios usan el segundo: Prometheus se actualiza cada 15 s y el
management API de RabbitMQ refresca sus estadísticas cada 5 s (la profundidad avanzaría en escalones).

## Métricas

| Métrica | Definición |
|---|---|
| `cold_start_s` | `startedAt` de meter-sim hasta el primer commit del gateway con el prefijo (`min(positions.updated_at)`) |
| `time_to_scale_up_s` | `startedAt` hasta la primera muestra con réplicas listas >= objetivo (`ceil(0.8 x SPIKE_RATE / C_POD)` = 5) |
| `peak_replicas`, `peak_depth` | máximos de `depth-k.csv` (réplicas deseadas y mensajes) |
| `drain_s` | `endedAt` de meter-sim hasta la primera muestra con profundidad <= 1 |
| `scale_to_zero_s` | primera muestra con profundidad <= 1 hasta la primera con 0 réplicas |
| Retraso en cola | p50/p99 de `outbox.created_at - occurredAt` por prefijo en `positions-db` (`occurredAt` lo fija meter-sim al enviar; es la espera en cola más el proceso) |
| `loss` | `published` del resumen de meter-sim menos filas de `positions` con el prefijo (más `outboxRows`, que debe ser igual) |
| Oscilación | número de bajadas de la serie de réplicas deseadas entre `startedAt` y `endedAt` |

`keda.json` no incluye `keda_scaler_active` / `keda_scaler_metrics_value`: el operador de KEDA corre con
`--enable-prometheus-metrics=false` en esta plataforma y no los expone. En su lugar `raw/keda-k-{scaledobject,hpa,events}.json`
guarda el estado del ScaledObject, el HPA y los eventos de Kubernetes al terminar la fase.

## Criterios y ajustes respecto al plan

Los umbrales de 03-experiments.md 5/E2 se mantienen sin cambios (`criteria.yaml`):

- Con 46 msg/s por réplica y máximo 10, 276 msg/s necesitan 6 réplicas, pero el trigger divide la profundidad entre `value` = 100.
  La cola crece mientras arrancan los pods (picos de 6000 mensajes), el HPA pide 10 réplicas (el máximo) y el criterio
  `peak_replicas >= 5` y `<= max` se cumple saturando el máximo. 10 réplicas (460 msg/s) tienen margen sobre los 276 msg/s del pico.
- El ScaledObject usa `value: 100` (`KEDA_QUEUE_VALUE` en `params.env`): ~2 s de cola por réplica con C_POD=46, por debajo del tope tolerado C_POD x 5 s = 230.
- `time_to_scale_up_s` se mide hasta réplicas listas (no deseadas): incluye arranque del pod con sidecar.
- Control: `drain_s` del control / `drain_s` de KEDA >= 3 y p99 de espera en cola > 30 s.

## Evidencia

`experiments/results/e02-keda-rabbit/<UTC>-<sha>-r<rep>/`: `metrics.json`, `result.json`, `raw/{depth,replicas}-{k,c}.csv`,
`raw/spike-{k,c}.json` (resumen de meter-sim), `raw/phase-{k,c}.json`, `raw/keda-k-*.json`, `figures/e02a-keda-vs-control.png`
(profundidad y réplicas x100, mismo eje, tiempo desde el inicio del pico), `figures/depth-{k,c}.png`, `logs/`.

## Resultado

Corrida que pasa (20/20 criterios): `experiments/results/final/e02-keda-rabbit/` (origen `20261017T060820Z-f02de76-r2`; el filtro de eventos y el
recorte de eventos se aplicó después a mano en la copia). Otra corrida con los mismos criterios (r1, desarrollo) dio valores parecidos.

| | KEDA | Control (1 réplica) |
|---|---|---|
| enviadas / filas en positions-db | 15 870 / 15 870 (pérdida 0) | 15 870 / 15 870 |
| `cold_start_s` | 14,25 (<= 20) | n/a |
| `time_to_scale_up_s` (listas >= 5) | 31 (<= 45) | n/a |
| pico de réplicas / profundidad | 10 / 6612 | 1 / 13 290 |
| `drain_s` | 18 (<= 60) | 320 (17,8x) |
| p99 retraso en cola | 24,2 s (<= 30) | 316 s |
| `scale_to_zero_s` | 34 (<= 60) | n/a |
| bajadas de réplicas en el pico | 0 | 0 |

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| E2A KEDA RabbitMQ | 0 -> N -> 0 siguiendo la cola, sin pérdida | cold start, escalado, drenado, scale-to-zero, pérdida | 20 s / 45 s / 60 s / 60 s / 0; control >= 3x | 14,25 s / 31 s / 18 s / 34 s / 0; control 17,8x: PASS (`results/final/e02-keda-rabbit/`) |
