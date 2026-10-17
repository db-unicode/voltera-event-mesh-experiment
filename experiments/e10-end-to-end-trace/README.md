# E10: seguimiento de un evento de punta a punta con KEDA en el camino

## Hipótesis

Un único `eventId` se puede seguir desde su emisión hasta un cambio observable, pasando por el broker, el consumidor escalado desde 0 por KEDA y el lado de lectura del CQRS.

## Procedimiento (`experiments/e10-end-to-end-trace/run.sh`)

1. **Reset:** `declaration-gateway` y `energy-transfer` de r1 en 0 réplicas (se espera el enfriamiento de KEDA, 30 s) y la zona `z-1` de la vista de r1 vacía (TTL 150 s), para que el par no se empareje con una posición ajena.
2. **Envío:** `meter-sim --profile pairs --zones 1 --vendor voltix-2 --meters 2 --rate 2 --duration 1s --log-publish` envía un SURPLUS y un DEFICIT en `z-1`. `--log-publish` (nuevo) escribe una línea de etapa con `eventId` y `traceId` por declaración.
3. **Seguimiento:** apenas el gateway y `energy-transfer` tienen un pod, se fija su ScaledObject en 1 réplica (`paused-replicas`) para que los logs no desaparezcan al volver a 0. El arranque en frío ya ocurrió. Se espera a que el match llegue a `device-sim` y a billing.
4. **Traza:** `expctl trace` por los `eventId` de las declaraciones y por el `matchId`; el `eventId` del match sale de la línea de commit del motor, porque matching-engine genera uno nuevo (enlace por `matchId`). La ronda final junta las cuatro claves. `analyze.jq` elige los hops y evalúa el orden.
5. **Arranque en frío:** eventos de k8s (`keda_events.txt`), creación y Ready del pod y primer log de arranque de la app.

## Cadena (hops)

| # | Hop | Servicio | Etapa | Broker que cruza |
|---|---|---|---|---|
| 1 | `sent` | meter-sim | publish | RabbitMQ |
| 2 | `gateway_consume` | declaration-gateway | consume (tras el arranque en frío) | |
| 3 | `gateway_commit` | declaration-gateway | commit en positions-db | |
| 4 | `gateway_publish` | declaration-gateway | publish (relay de la outbox) | RabbitMQ |
| 5 | `projector_consume` | market-projector | consume | |
| 6 | `projector_apply` | market-projector | apply en Redis | |
| 7 | `match_commit` | matching-engine | commit en matches-db | |
| 8 | `match_publish` | matching-engine | publish (relay) | Kafka |
| 9 | `transfer_consume` | energy-transfer | consume (arranque en frío) | |
| 10 | `device_command` | device-sim | command | |
| 11 | `transfer_command` | energy-transfer | command (tras el 2xx del dispositivo) | |
| 12 | `bridge_consume` | mesh-bridge | consume | |
| 13 | `bridge_publish` | mesh-bridge | publish (tras el PubAck) | NATS |
| 14 | `billing_received` | billing-sim | received | |

Cada hop lleva las restricciones de orden de `analyze.jq` (`after`). `billing_received` va después de `bridge_consume` y no después de `bridge_publish`: el stream confirma al bridge y entrega a billing casi a la vez, y el log del bridge sale tras el PubAck (en la traza `bridge_publish` aparece unos 0,5 ms después de `billing_received`). Los hops 5 y 9 se comparan con el commit anterior, no con el publish del relay: el relay escribe su log tras el confirm del broker y el consumidor puede registrar el mensaje antes.

El par genera dos cadenas hasta el motor (una por declaración). La cadena elegida es la de la declaración cuya aplicación en Redis cierra el par.

## Medición

- Orden: cada hop respeta las restricciones; los timestamps son los del campo `ts` del log (ns).
- Descomposición: tramos entre hops consecutivos de la rama más larga (la de `device_command` o la de `billing_received`); su suma se compara con la latencia total.
- Arranque en frío: `keda_detect_and_scale` (envío o commit → creación del pod), `pod_start_to_app_ready` (creación → log de arranque de la app) y `app_ready_to_first_message`. Los eventos de k8s tienen resolución de 1 s: las anclas se acotan para ser monótonas entre los dos hops que las rodean.

## Criterio

Ver `criteria.yaml`: ≥ 12 hops presentes y en orden, ≥ 5 servicios, ≥ 2 brokers, tramos que suman la latencia total ±5 %, arranque en frío del gateway visible y eventos de k8s y de KEDA capturados.

## Desviaciones

- **Jaeger y Kiali:** el repo no usa el SDK de OpenTelemetry (el `traceparent` viaja por el sobre y los headers, pero no hay spans), así que no hay traza en Jaeger ni captura de Kiali. La evidencia es `expctl trace` (`trace_<id>.txt/json`) y `figures/hops.png`.
- **Pausa de KEDA tras el arranque:** el ScaledObject se fija en 1 réplica después de que el pod existe (para conservar sus logs); se reanuda al terminar la corrida.
- La traza por el `matchId` solo trae 7 hops: el log del relay del motor (`publish`) lleva el `eventId` del match, no el `matchId`.

## Evidencia

`trace_<eventId>.txt/json` (cada declaración y el matchId), `trace_all.txt/json`, `hops.csv`, `keda_events.txt`, `metrics.json`, `result.json`, `figures/hops.png`.

## Resultado

Corrida `20261017T062645Z-b5e155b-r1` (veredicto PASS). Evidencia versionada en `experiments/results/final/e10-end-to-end-trace/`.

| Criterio | Observado | Umbral | Veredicto |
|---|---|---|---|
| El par enviado produce exactamente un match entre sus dos medidores (zona única sin posiciones ajenas) | 1 | eq 1 | PASS |
| Hops de la cadena presentes en los logs (>= 12) | 14 | ge 12 | PASS |
| Todos los hops respetan su orden causal (1 = en orden estrictamente creciente de tiempo) | 1 | eq 1 | PASS |
| Servicios distintos en la cadena (>= 5) | 8 | ge 5 | PASS |
| Brokers por los que pasa la cadena (>= 2) | 3 | ge 2 | PASS |
| Error entre la suma de los tramos y la latencia total (%) | 0 | le 5 | PASS |
| Error entre los componentes del arranque en frío del gateway y su total (%) | 0 | le 5 | PASS |
| El gateway arrancó en frío (s entre el envío y su primer consumo; > 2 s) | 5.1 | gt 2 | PASS |
| Error entre los componentes del arranque en frío de energy-transfer y su total (%) | 0 | le 5 | PASS |
| Eventos de k8s del arranque del gateway y de energy-transfer capturados (pods iniciados) | 6 | ge 2 | PASS |
| Eventos de activación de KEDA capturados (KEDAScaleTargetActivated) | 2 | ge 2 | PASS |
| Latencia total de la cadena, del envío a su último hop (s; arranques en frío incluidos) | 9.08 | le 120 | PASS |

Notas de la corrida:

- 14 hops, 8 servicios (billing-sim, declaration-gateway, device-sim, energy-transfer, market-projector, matching-engine, mesh-bridge, meter-sim), brokers kafka, nats, rabbitmq. Todos respetan su orden causal; latencia total 9,08 s hasta `device_command` (rama de transferencia 9,08 s, rama del mesh 5,57 s).
- Arranque en frío del gateway: 5,10 s (keda_detect_and_scale 3,13 s, pod_start_to_app_ready 1,90 s, app_ready_to_first_message 0,06 s). De `energy-transfer`: 3,52 s (keda_detect_and_scale 0,00 s, pod_start_to_app_ready 3,51 s, app_ready_to_first_message 0,02 s).

| Tramo | Hops | s |
|---|---|---|
| cold_start_gateway | sent → gateway_consume | 5,098 |
| gateway_process | gateway_consume → gateway_commit | 0,002 |
| outbox_to_projection | gateway_commit → projector_apply | 0,002 |
| match_window | projector_apply → match_commit | 0,450 |
| kafka_to_transfer | match_commit → transfer_consume | 3,521 |
| transfer_to_device | transfer_consume → device_command | 0,004 |

## Fila de la tabla final

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| **E10 Seguimiento de un evento:** un `eventId` con gateway en 0 réplicas. **Patrones:** EDA + KEDA + CQRS + trazabilidad. | Se puede seguir el evento de punta a punta. | Nº de hops, orden temporal, latencia por tramo. | >= 12 hops ordenados, >= 5 servicios, 2 brokers, suma +/- 5 %. | 14 hops en orden, 8 servicios, 3 brokers, suma de tramos error 0 %, 9,08 s con arranque en frío de 5,10 s; PASS (`experiments/results/final/e10-end-to-end-trace/`) |
