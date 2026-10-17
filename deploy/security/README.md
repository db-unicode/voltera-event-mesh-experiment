# Seguridad (Zero Trust L4)

`mise run security:up [r1|r2|all]` aplica `deploy/security/<región>/`. Hoy:

| Política | Destino | Permite |
|---|---|---|
| `positions-db` (ALLOW, puerto 5432) | `positions-db` de cada región | solo `cluster.local/ns/voltera-<r>/sa/declaration-gateway` |

- Rechazo: el sidecar de `positions-db` cierra la conexión (contador Envoy `tcp.rbac.denied`).
- No afecta a `kubectl exec` ni a `kubectl port-forward` (entran por el loopback del pod, sin pasar por el filtro RBAC),
  así que `expctl sql`, los spikes y `mise run data:test` siguen funcionando.
- Si añades otro cliente legítimo de `positions-db` (un Job de migración con otro ServiceAccount, p. ej.), añade su
  principal en `deploy/security/positions-db/authorizationpolicy.yaml`. Hoy las migraciones las ejecuta el propio gateway.
- El RabbitMQ compartido tiene su política en `deploy/brokers/rabbitmq/security.yaml`.
