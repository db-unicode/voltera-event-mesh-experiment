# Aplicaciones

Estructura: `base/<servicio>/` (Deployment, ServiceAccount, Service si expone HTTP, ScaledObject si escala por KEDA) y
overlays `<region>/<grupo>/kustomization.yaml` que fijan namespace, `REGION`, topic y credenciales. Mismos nombres de
recursos en r1 y r2.

## Grupo `match-side` (r1, r2) y `billing` (global)

`mise run apps:match-side [r1|r2|all]` construye las imágenes, aplica los overlays y espera los rollouts. billing-sim
(`deploy/apps/global/billing/`) se despliega siempre, porque lo comparten las dos regiones.

| Servicio | Réplicas | Entrada | Salida | Puertos |
|---|---|---|---|---|
| `matching-engine` | 1 (particionar por zona permitiría N) | vista Redis `market-view` (solo lectura), reglas `matching-rules` | matches-db + outbox -> Kafka `<region>.energy-matched` | `http-ops` 9090 |
| `energy-transfer` | 0 a 6 (KEDA, lag Kafka) | Kafka `<region>.energy-matched`, grupo `energy-transfer` | transfer-db; comando HTTP firmado a device-sim | `http-ops` 9090 |
| `device-sim` | 1 | `POST /commands` | estado en memoria (`GET /state`) | `http-api` 8080, `http-ops` 9090 |
| `mesh-bridge` | 1 | Kafka `<region>.energy-matched`, grupo `mesh-bridge` | NATS regional `energy.matched.<region>.<matchId>` | `http-ops` 9090 |
| `billing-sim` (global) | 1 | durable `billing` del stream `BILLING_MATCHED` | estado en memoria (`GET /state`) | `http-api` 8080, `http-ops` 9090 |

`http-ops` sirve `/healthz`, `/readyz`, `/metrics`, `/debug/state` y, con `EXPERIMENTS=1`, `/admin/failpoint`. Los
Deployments de experimento llevan `EXPERIMENTS=1`.

### Reglas de matching

ConfigMap `matching-rules` (archivo `rules.yaml`, montado como directorio en `/etc/matching-rules`, sin `subPath`; el
nombre no lleva hash para que el pod no se reinicie al cambiarlo). Fuente: `base/matching-engine/rules.cfg`.

```yaml
params:                 # constantes visibles como "params"
  minKwh: 0.1
  maxPricePerKwh: 1.0
eligible: "position.kwh >= params.minKwh && price.pricePerKwh <= params.maxPricePerKwh"   # por posición
pair: "seller.zone == buyer.zone && seller.meterId != buyer.meterId"                       # por par
```

- `eligible` ve `position`, `price`, `params`; `pair` ve `seller`, `buyer`, `price`, `params`. Ambas deben devolver `bool`.
- `position`/`seller`/`buyer`: `meterId`, `side`, `kwh`, `validUntil`, `zone`, `version`. `price`: `pricePerKwh`, `validUntil`, `version`.
- El motor lee el archivo cada 2 s (`RULES_POLL_S`) y compara su sha256. Si el contenido nuevo compila lo publica de
  forma atómica; si no (YAML inválido, campo desconocido, variable inexistente, resultado no booleano), conserva las
  reglas anteriores y lo registra (`matching_rules_reload_total{result="error"}`, `/debug/state` -> `rules.lastReloadError`).
- El kubelet tarda hasta ~1 min en reflejar un cambio del ConfigMap en el volumen.

### Política de fallos de energy-transfer

El comando se envía como mucho una vez por `matchId`. Si el dispositivo no responde 2xx, la transferencia queda `FAILED`,
se registra `TransferFailed` y no se reintenta: repetir un comando físico a ciegas puede transferir energía de más. Una
transferencia `PENDING` huérfana (crash entre registrarla y enviar) tampoco se reenvía; se ve en `/debug/state`
(`transfers`). Los mensajes que no son un `EnergyMatched` válido se saltan con un log de error (no bloquean la partición).
