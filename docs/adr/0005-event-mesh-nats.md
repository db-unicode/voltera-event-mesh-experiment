# 0005. Event mesh NATS: hub con leafnodes

- Estado: aceptada (ajusta el detalle de 0003: leafnodes en lugar de gateways, una sola cuenta)
- Fecha: 2026-10-17

## Contexto y problema

Cada región publica `EnergyMatched`; Facturación (global) debe recibir los de ambas sin conocerlas, y nada se puede perder si el enlace entre regiones se corta. Los informes proponían topologías distintas: hub con leafnodes (S8) o gateways (stack tecnológico).

## Decisión

- Tres nodos NATS 2.15.1 (`nats` en `voltera-r1`, `voltera-r2`, `voltera-global`). Hub y spokes: R1 y R2 abren un leafnode saliente hacia el global (`tcp-leafnodes` 7422). Una cuenta `MESH`, identidad por usuario.
- JetStream solo en el global: stream `BILLING_MATCHED` (`energy.matched.>`, límites 7 días / 512 MiB, `duplicate_window` 2 min) y consumer durable `billing` (pull, ack explícito).
- El bridge regional publica en su nodo local con `js.Publish`. La petición cruza el leafnode, el stream la guarda y el `PubAck` vuelve por el mismo enlace. Sin enlace no hay respuesta: el publicador recibe `no response from stream` y no confirma el offset de Kafka.
- Permisos por usuario (config estática en Secret de kustomize): `bridge-r1` publica `energy.matched.r1.>`, `bridge-r2` `energy.matched.r2.>`, `billing` solo su durable y suscribe `energy.matched.>`, `rogue` nada. El enlace se autentica con `leaf-r1`/`leaf-r2`, que en el hub solo publican su región: un nodo regional comprometido no puede inyectar eventos de otra.

## Por qué leafnodes y no gateways

- El flujo es unidireccional hacia el global; no hay tráfico R1-R2. Con gateways cada nodo sería un cluster de un servidor con enlaces a los otros dos (malla completa) y el global tendría que unirse a un super-cluster solo para recibir.
- Los gateways unen clusters pares ([docs](https://docs.nats.io/running-a-nats-service/configuration/gateways)); un leafnode es un servidor que abre una conexión saliente y extiende al hub ([docs](https://docs.nats.io/running-a-nats-service/configuration/leafnodes)). Encaja con una región que sigue aceptando publicaciones locales aunque el global caiga.
- Los usuarios de enlace permiten acotar en el hub qué subjects inyecta cada spoke; el enlace de gateway no lleva permisos por subject.
- El PubAck de JetStream funciona sin dominios ni exports/imports porque R1, R2 y el global comparten cuenta y el stream vive en el hub.

## Alternativas descartadas

- Cuentas separadas `R1`, `R2`, `BILLING` con exports/imports de servicio: aísla más, pero hace falta un import por región para que el stream capture y el PubAck mapee la respuesta, y cada cuenta tiene que estar también en el hub. Con un solo bridge por región, los permisos por usuario en ambos extremos dan el mismo resultado en este alcance.
- JetStream en cada región con `source` hacia el global (dominios): da buffer en la región, pero ya existe en Kafka regional y duplicaría estado y complejidad. El ACK del stream global es lo que habilita el commit del offset.
- JWT/`nsc`: da revocación y firmas, pero cuesta un operador y claves. Las credenciales de usuario/clave del repo son de desarrollo.

## Límites declarados

- Credenciales de desarrollo en el repo; en producción, NKeys/JWT o secretos externos.
- Las regiones comparten cluster, nodo y reloj: la partición se simula con una `AuthorizationPolicy` DENY al puerto 7422 y reinicio del nodo regional (el DENY L4 no corta conexiones ya establecidas).
- `billing` conserva permiso de suscribir `energy.matched.>` en core NATS (sin durabilidad), como pide el diseño; la entrega fiable es por el durable.
- `duplicate_window` es finito: el consumidor de Facturación debe seguir siendo idempotente.
- JetStream en un solo nodo y PVC `local-path`: no hay replicación.

## Consecuencias

- Tres pods ~177 MiB con sidecars (nats 9-33 MiB, istio-proxy ~40 MiB cada uno).
- Puertos con prefijo `tcp-` (`tcp-client`, `tcp-leafnodes`) y `http-monitor`: NATS es server-first y Istio no debe detectar el protocolo.
- Las pruebas están en S8 (`experiments/spikes/s08-nats-mesh/`).
