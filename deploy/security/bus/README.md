# Zero Trust del bus

Políticas de Istio por identidad (SPIFFE de cada ServiceAccount) y puerto sobre Kafka, RabbitMQ y NATS. Complementan, no
reemplazan, los permisos por usuario de RabbitMQ y NATS. Las usa el experimento E9 (`experiments/e09-zero-trust/`).

```sh
mise run security:bus on       # kubectl apply -k deploy/security/bus
mise run security:bus status
mise run security:bus off      # borra todo lo etiquetado zero-trust=bus
```

El experimento las aplica al empezar y las borra al terminar (`E09_KEEP_POLICIES=1` las deja). Por defecto están apagadas: un
Job de otro experimento con un ServiceAccount que no figure en las listas perdería el acceso a los brokers.

| Archivo | Política | Efecto |
|---|---|---|
| `kafka.yaml` | ALLOW en `kafka`, 9092 | `matching-engine`, `energy-transfer`, `mesh-bridge` (r1 y r2), `keda-operator` y `voltera-infra/default` (kafka-exporter, topics, broker). El resto de puertos queda abierto a identidades mTLS |
| `rabbitmq.yaml` | DENY en `rabbitmq`, 5672 | Rechaza todo ServiceAccount que no sea `meter-sim`, `declaration-gateway`, `price-feed-simulator` o `market-projector`. DENY porque el ALLOW existente (`deploy/brokers/rabbitmq/security.yaml`) admite cualquier identidad y las ALLOW se suman |
| `nats.yaml` | ALLOW en cada `nats` | 4222: solo `mesh-bridge` de la región (global: `billing-sim` y `default`, el Job de `mesh:up`); 7422 del global: solo los nodos regionales; resto de puertos abierto a mTLS |

## Límite

Istio decide por identidad y puerto, no por topic ni por cola. En Kafka, quien entra por el 9092 puede producir y consumir en
cualquier topic; el control por topic exige SASL y ACL de Kafka. En RabbitMQ y NATS el control fino por cola o subject lo dan los
permisos de cada usuario.

## Añadir una identidad

- Kafka y NATS: crea otra `AuthorizationPolicy` ALLOW con el ServiceAccount (las ALLOW se suman).
- RabbitMQ: añade el principal a `notPrincipals` de `rabbitmq.yaml`.

`voltera-infra/default` es el ServiceAccount de varios pods de infraestructura (broker, exporter, Jobs); darle a kafka-exporter
un ServiceAccount propio permitiría quitarlo de la lista.
