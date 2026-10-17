# S8. Event mesh NATS entre dos regiones con autorización

Tres nodos NATS 2.15.1 (`nats` en `voltera-r1`, `voltera-r2` y `voltera-global`) en topología hub con leafnodes ([ADR 0005](../../../docs/adr/0005-event-mesh-nats.md)). JetStream solo en el global.

## Ejecutar

```sh
mise run spike:s08   # hace mesh:up, construye mesh-probe y ejecuta los checks
```

`mesh:up` es idempotente: aplica los 3 nodos (`deploy/mesh/nats/{r1,r2,global}`) y recrea el Job `nats-setup` (`deploy/mesh/nats/setup`) que crea el stream `BILLING_MATCHED` y el consumer durable `billing`. Evidencia en `experiments/results/s08/<UTC>-<sha>/`.

## Identidades (config estática en un Secret de kustomize)

| Usuario | Nodo | Permisos |
|---|---|---|
| `bridge-r1` | `nats-r1` | publica `energy.matched.r1.>`; suscribe `_INBOX.>` (PubAck) |
| `bridge-r2` | `nats-r2` | publica `energy.matched.r2.>`; suscribe `_INBOX.>` |
| `billing` | `nats-global` | publica solo `$JS.API.CONSUMER.{INFO,MSG.NEXT}` y `$JS.ACK` de su durable; suscribe `energy.matched.>` e `_INBOX.>` |
| `rogue` | los tres | deny `>` en publicar y suscribir |
| `leaf-r1`, `leaf-r2` | `nats-global` (enlace) | cada uno publica solo su región; suscribe `_INBOX.>` |
| `admin` | `nats-global` | sin límites (Job de setup y evidencia) |

## Checks

| Check | Qué comprueba |
|---|---|
| a | `/leafz` de los 3 nodos (JSON guardado) y que el INFO server-first llega por el sidecar en 4222 y 7422 |
| b | 100 + 100 publicados con PubAck del stream global; `billing` recibe 200 distintos; el stream guarda 200 |
| c | `bridge-r2` en `energy.matched.r1.x`, `rogue` (publicar, suscribir, durable) y `billing` publicando: violation y 0 mensajes nuevos |
| d | Mismo `Nats-Msg-Id` dos veces: 2º PubAck con `duplicate`, el stream guarda 1 |
| e | `AuthorizationPolicy` DENY al 7422 desde R2 y reinicio de `nats-r2`: publicar falla sin ACK, R1 sigue; al restaurar, reintento con los mismos ids: 20 ACK, sin duplicados |
| f | Pod sin sidecar no recibe INFO ni conecta (mTLS STRICT), con controles positivos |

## Hallazgos

- **Permisos de enlace:** el bloque `leafnodes.authorization` no admite `permissions`. Los usuarios de enlace se definen en la cuenta y el hub no declara `authorization`; así los permisos aplican al leafnode.
- **Violaciones asíncronas:** el servidor responde un publish prohibido con `-ERR Permissions Violation` y no con un PubAck; `mesh-probe` las cuenta y no reintenta.
- **Partición:** el DENY L4 de Istio no corta conexiones establecidas; se reinicia `nats-r2` para tirar el leafnode. Sin enlace, `js.Publish` devuelve `nats: no response from stream` (~2 s) y no hay ACK.
- **Jobs con sidecar:** terminan sin trucos (sidecar nativo de Istio).
- **Monitor:** el kubelet y el apiserver no están en el mesh, así que `/leafz` se lee desde un pod con sidecar o con `kubectl exec -c istio-proxy -- curl localhost:8222/...`.

## Conectar el futuro `mesh-bridge`

- URL: `nats://nats.voltera-<r1|r2>.svc.cluster.local:4222` (nodo de su región, nunca el global).
- Credenciales: usuario `bridge-<región>` y su clave por `NATS_USER`/`NATS_PASSWORD` desde un Secret propio del namespace de la región.
- Subject: `energy.matched.<región>.<matchId>`; cabecera `Nats-Msg-Id` = `matchId`.
- ACK: publicar con `jetstream.PublishMsg` (paquete `jetstream` de nats.go) y confirmar el offset de Kafka solo cuando vuelve `PubAck` sin error. Cualquier error (`no response from stream`, timeout) significa no confirmar y reintentar el mismo `Nats-Msg-Id`: el stream deduplica durante 2 min. `jetstream.WithRetryAttempts(0)` y reintento propio con backoff, como en `internal/meshprobe`.
