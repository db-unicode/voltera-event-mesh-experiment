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

<!-- RESULT -->
