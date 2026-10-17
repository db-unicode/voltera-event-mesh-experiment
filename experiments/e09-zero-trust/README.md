# E9: Zero Trust sobre el bus

## Hipótesis

Solo la identidad prevista publica o suscribe cada canal. El resto, incluidos los pods fuera del mesh, es rechazado.

## Capas

| Capa | Dónde | Decide por |
|---|---|---|
| mTLS STRICT | todo el mesh (`istio-system/default`) | el pod tiene sidecar y certificado |
| Política L4 de Istio | `deploy/security/bus/` (`mise run security:bus on\|off`) | ServiceAccount (SPIFFE) y puerto. No ve topics, colas ni subjects |
| ACL del broker | RabbitMQ: permisos por usuario y vhost. NATS: usuarios y permisos por subject. Kafka: no hay | usuario y credencial |

## Variables

- **Identidades** (`zt-identities.csv`): los 9 servicios con su ServiceAccount real (`meter-sim`, `declaration-gateway`, `price-feed-simulator`, `market-projector`, `matching-engine`, `energy-transfer`, `device-sim`, `mesh-bridge`, `billing-sim`) y, cada uno con su credencial si la tiene; una SA ajena con la credencial `rogue` (`stranger`); dos ladrones de credencial (`thief-rabbit`: SA `energy-transfer` con la credencial de `meter-sim`; `thief-nats`: SA `device-sim` con la de `bridge-r1`); una contraseña incorrecta (`badpass-rabbit`); `mesh-bridge` sin credencial (`nocred-nats`); y un pod sin sidecar (`plain`, namespace `e09-plain`) con credenciales válidas.
- **Canales y operaciones** (`zt-matrix.csv`): RabbitMQ vhost r1 (`meter-declarations`, `energy-positions`, `kwh-prices` x publicar/consumir), Kafka (`r1.energy-matched`, `r2.energy-matched` x producir/consumir), NATS (conectar, publicar en `energy.matched.r1.*` y `r2.*`, suscribir `energy.matched.>`, `consumer-info` del durable `billing`).
- **Columnas de la matriz:** `design` es la intención de negocio (la tabla de permitidos de `03-experiments.md`: 14 celdas legítimas); `expected` es lo que deben hacer las capas configuradas (política L4 y ACL: se rechaza si falla cualquiera); `layer` dice cuál lo rechaza primero (`istio-l4`, `mtls`, `broker-acl`, `broker-authn`).
- **Límite declarado:** Istio decide por identidad y puerto. En Kafka las celdas `design=deny, expected=allow` (9: `matching-engine` consumiendo, `energy-transfer` y `mesh-bridge` produciendo, y los tres sobre el topic de la otra región) no se pueden imponer sin SASL y ACL de Kafka. Se miden y se confirma que se comportan como se predijo.

## Procedimiento (`experiments/e09-zero-trust/run.sh`)

1. **Reset:** imágenes `zt-probe` y `meter-sim`, SA `zt-stranger`, namespace `e09-plain` sin sidecar con copia de dos Secrets válidos, políticas del bus fuera.
2. **Control sin políticas:** `thief-rabbit` y `thief-nats` repiten sus celdas. Con una credencial robada, el broker solo mira el usuario, así que la publicación legítima de ese usuario pasa desde otra SA.
3. **Se aplican las políticas** (`security:bus on`, 20 s de propagación) y se capturan los contadores `rbac...denied` de los sidecars de Kafka, RabbitMQ y los tres NATS.
4. **Matriz:** un Job por identidad (ServiceAccount real, `voltera/zt-probe:dev`) intenta todas sus celdas. Una celda es `permitida` si el broker completó la operación y `rechazada` ante cualquier error.
5. **Flujo legítimo con las políticas aplicadas:** `pairs` en r1 (5 pares/s) y r2 (2 pares/s) durante 20 s. Cada match debe llegar a billing y a `device-sim`. Esto ejercita a la vez `keda-operator` hacia Kafka (escalado de `energy-transfer` desde 0), `kafka-exporter`, el leafnode y los consumidores.
6. **Se quitan las políticas** (salvo `E09_KEEP_POLICIES=1`).

Efectos laterales de las celdas permitidas: un mensaje de prueba ilegible en el stream `BILLING_MATCHED` y en los topics de Kafka (billing, `mesh-bridge` y `energy-transfer` lo descartan con un log de error) y, en RabbitMQ, la publicación va a una clave sin cola (se descarta) y el consumo devuelve a la cola (`requeue`) como mucho un mensaje.

## Medición

`zt_matrix_result.csv`: esperado, real y veredicto (`match`, `false_negative`: acceso indebido concedido, `false_positive`: acceso legítimo denegado) por celda. `rbac_denied.txt`: contadores `tcp.rbac.denied` antes y después. `logs/`: logs de `nats-server` y RabbitMQ. Kafka no registra rechazos de autenticación porque no autentica: el rechazo ocurre en el sidecar.

## Criterio

Ver `criteria.yaml`: 0 falsos negativos y 0 falsos positivos respecto de lo esperado, las 14 celdas legítimas permitidas, 0 operaciones exitosas desde el pod sin sidecar, el límite de Kafka confirmado (9 celdas), el control de credencial robada, `rbac denied` > 0 en los tres brokers, flujo legítimo intacto (0 matches perdidos en billing ni en device-sim) y 0 políticas al terminar.

## Evidencia

`zt_matrix_result.csv`, `zt-matrix.csv`, `rbac_denied.txt`, `metrics.json`, `result.json`, `raw/zt-*.jsonl` (resultado por celda de cada Job), `raw/policies.txt`, `logs/authorizationpolicies.yaml`.

<!-- RESULT -->
