# 0004. Regiones como namespaces

- Estado: aceptada
- Fecha: 2026-10-16

## Contexto y problema

El diseño tiene Región 1, Región 2 y un Facturación global. Dos clusters kind cuestan otro control-plane (+0,9 GiB), un Istio multi-primary y una raíz de certificados compartida.

## Decisión

Un cluster, un namespace por región:

- `voltera-r1`, `voltera-r2`: servicios y NATS de cada región.
- `voltera-global`: Facturación y `nats-global`.
- `voltera-platform`: Kafka, RabbitMQ y observabilidad compartida.

Cada namespace lleva `PeerAuthentication` STRICT y `AuthorizationPolicy` default-deny con allow por ServiceAccount. Kafka y RabbitMQ son compartidos: topics con prefijo de región (`r1.`, `r2.`) y vhosts `r1`/`r2` en RabbitMQ.

## Límite declarado

- Las regiones comparten broker, nodo y reloj. No hay fallo independiente de región; la partición se simula con `AuthorizationPolicy` DENY o toxiproxy.
- La latencia entre regiones es simulada.
- Un Kafka por región (+0,8 GiB cada uno) o dos clusters kind quedan como trabajo futuro.

## Consecuencias

- Misma RAM que el diseño de un solo cluster (~5,6 GiB estimados).
- NetworkPolicy en kind (kindnet) puede no aplicarse; el aislamiento se apoya en Istio L4 y la prueba se hace en el spike S9.
