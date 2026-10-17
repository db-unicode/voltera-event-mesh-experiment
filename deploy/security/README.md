# Seguridad (Zero Trust L4)

Dos grupos de políticas de Istio por identidad (SPIFFE del ServiceAccount):

| Grupo | Task | Estado por defecto | Qué cubre |
|---|---|---|---|
| `deploy/security/<región>/` | `mise run security:up [r1\|r2\|all]` | aplicado | Database per Service: solo el dueño abre conexiones a su base (E4) |
| `deploy/security/bus/` | `mise run security:bus on\|off\|status` | apagado (E9 lo enciende) | quién publica y quién se suscribe en Kafka, RabbitMQ y NATS ([README](bus/README.md)) |

Políticas de `security:up`:

| Política | Destino | Permite |
|---|---|---|
| `positions-db` (ALLOW, puerto 5432) | `positions-db` de cada región | solo `cluster.local/ns/voltera-<r>/sa/declaration-gateway` |

- Rechazo: el sidecar de `positions-db` cierra la conexión (contador Envoy `tcp.rbac.denied`).
- No afecta a `kubectl exec` ni a `kubectl port-forward` (entran por el loopback del pod, sin pasar por el filtro RBAC),
  así que `expctl sql`, los spikes y `mise run data:test` siguen funcionando.
- Si añades otro cliente legítimo de `positions-db` (un Job de migración con otro ServiceAccount, p. ej.), añade su
  principal en `deploy/security/positions-db/authorizationpolicy.yaml`. Hoy las migraciones las ejecuta el propio gateway.
- El RabbitMQ compartido tiene su política en `deploy/brokers/rabbitmq/security.yaml`.
