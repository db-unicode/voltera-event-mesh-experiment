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

- **Identidades** (`zt-identities.csv`): los 9 servicios con su ServiceAccount real (`meter-sim`, `declaration-gateway`, `price-feed-simulator`, `market-projector`, `matching-engine`, `energy-transfer`, `device-sim`, `mesh-bridge`, `billing-sim`), cada uno con su propia credencial si la tiene; una SA ajena con la credencial `rogue` (`stranger`); dos ladrones de credencial (`thief-rabbit`: SA `energy-transfer` con la credencial de `meter-sim`; `thief-nats`: SA `device-sim` con la de `bridge-r1`); una contraseña incorrecta (`badpass-rabbit`); `mesh-bridge` sin credencial (`nocred-nats`); y un pod sin sidecar (`plain`, namespace `e09-plain`) con credenciales válidas.
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

## Resultado

Corrida final `20261017T074315Z-18789b0-dirty-r1` (`experiments/results/final/e09-zero-trust/`, ronda `final-run-18789b0.tsv`): 19/19 criterios en PASS. La marca `-dirty` se debe solo a la evidencia promovida a `results/final` durante la corrida; el código es el del commit indicado.

| Criterio | Observado | Umbral | Veredicto |
|---|---|---|---|
| Celdas de la matriz sin resultado (el Job no las ejecutó) | 0 | eq 0 | PASS |
| Falsos negativos (acceso indebido concedido, respecto de lo esperado) | 0 | eq 0 | PASS |
| Falsos positivos (acceso legítimo denegado) | 0 | eq 0 | PASS |
| Celdas cuyo resultado real difiere del esperado | 0 | eq 0 | PASS |
| Celdas legítimas del diseño que el bus permitió (de 14) | 14 | eq 14 | PASS |
| Operaciones exitosas desde el pod sin sidecar (con credenciales válidas) | 0 | eq 0 | PASS |
| Celdas intentadas desde el pod sin sidecar | 15 | ge 15 | PASS |
| Celdas Kafka que Istio no distingue por topic (diseño deny, L4 allow) y se comportaron como se predijo | 9 | eq 9 | PASS |
| Control sin políticas - credencial de meter-sim desde otra SA, publicación en RabbitMQ | 1 | eq 1 | PASS |
| Control sin políticas - credencial de bridge-r1 desde otra SA, publicación en NATS | 1 | eq 1 | PASS |
| `rbac denied` del sidecar de Kafka (incremento) | 44 | ge 1 | PASS |
| `rbac denied` del sidecar de RabbitMQ (incremento) | 42 | ge 1 | PASS |
| `rbac denied` de los sidecars de NATS (incremento) | 45 | ge 1 | PASS |
| Flujo legítimo con las políticas aplicadas: matches en r1 | 100 | ge 50 | PASS |
| Flujo legítimo con las políticas aplicadas: matches en r2 | 40 | ge 20 | PASS |
| Matches de r1 que no llegaron a billing | 0 | eq 0 | PASS |
| Matches de r2 que no llegaron a billing | 0 | eq 0 | PASS |
| Matches sin efecto en device-sim (energy-transfer escalado por KEDA bajo las políticas) | 0 | eq 0 | PASS |
| Políticas del bus que quedan al terminar | 0 | eq 0 | PASS |

Notas de la corrida:

- 187 celdas, 187 con el resultado esperado. Por broker: kafka 44/44, nats 65/65, rabbitmq 78/78. Por capa que rechaza (`-` = permitida): 23 permitidas, 23 `broker-acl`, 11 `broker-authn`, 115 `istio-l4`, 15 `mtls`.
- Pod sin sidecar: 0 de 15 celdas exitosas con credenciales válidas.
- Límite de Kafka: 9 celdas con `design=deny` que Istio permite (mismo ServiceAccount, otro topic u operación); todas se comportaron como se predijo.
- Flujo legítimo con las políticas aplicadas: r1 100 matches y r2 40, todos en billing y en `device-sim`.

## Fila de la tabla final

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| **E9 Zero Trust en el bus:** matriz identidad x canal x operación en RabbitMQ/Kafka/NATS. **Patrones:** Zero Trust, mTLS STRICT, AuthorizationPolicy. | Solo la identidad prevista publica/suscribe. | Celdas esperadas vs reales; accesos desde pod sin sidecar. | 0 falsos negativos y 0 falsos positivos; 0 conexiones sin mTLS. | 187/187 celdas como se esperaba (0 falsos negativos, 0 falsos positivos); pod sin sidecar 0/15 exitosas; límite Kafka por topic: 9 celdas; `rbac denied` 44 / 42 / 45 (Kafka / RabbitMQ / NATS); PASS 19/19 (`experiments/results/final/e09-zero-trust/`, corrida `18789b0-dirty-r1`) |
