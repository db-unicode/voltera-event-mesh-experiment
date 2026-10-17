# 0003. Tres brokers con papeles distintos

- Estado: aceptada (topología del mesh precisada en [0005](0005-event-mesh-nats.md))
- Fecha: 2026-10-16

## Contexto y problema

La rúbrica pide distinguir cola, topic con log y event mesh. Un solo producto para los tres papeles dejaría sin evidencia la diferencia semántica.

## Decisión

| Papel | Producto | Canales | Por qué |
|---|---|---|---|
| Cola durable | RabbitMQ 4.3.6, quorum queues | `meter-declarations`, `energy-positions`, `kwh-prices` | El mensaje se borra al hacer ack, sin replay ni fan-out. Scaler `rabbitmq` nativo de KEDA |
| Topic con log retenido | Apache Kafka 4.3.1, KRaft, 1 broker (StatefulSet propio) | `energy-matched` (6 particiones) | El log se conserva y cada grupo lleva su offset: replay y fan-out. Scaler `apache-kafka` por lag |
| Event mesh | NATS 2.15.1: un nodo por región y uno global, unidos por leafnodes ([ADR 0005](0005-event-mesh-nats.md)) | `energy.matched.<region>.<matchId>` | Red de brokers con enrutamiento por interés, permisos por usuario, 30-60 MiB por nodo |

`mesh-bridge` (Go) lee `energy-matched` en Kafka y publica en el NATS local; no hay dual-write desde el dominio.

## Alternativas descartadas

- Solace PubSub+ Standard con DMR: mayor fidelidad al término "event mesh", pero ~5 GiB para dos brokers y cliente Go con CGO.
- Strimzi: +0,9 GiB de RAM y sin soporte oficial con Istio.
- MirrorMaker 2, Knative Eventing, Dapr, Pulsar: replican o abstraen, no forman un mesh heterogéneo, o pesan demasiado.

## Consecuencias

- Presupuesto de RAM estimado ~5,6 GiB (~7,3 GiB con 30 % de margen); S0 lo mide.
- Istio ve los tres brokers como TCP: da mTLS e identidad, no traza ni distingue topics. El hilo del evento va en `eventId` y `traceparent` dentro del mensaje.
- Un quorum queue de un nodo no replica (réplica 1); se usa por ser el tipo recomendado en 4.x.
