# E8: event mesh multi-región con autorización

## Hipótesis

Ambas regiones publican `EnergyMatched` al mesh y Facturación (único suscriptor global) los recibe. Solo los publicadores y suscriptores autorizados operan. Una partición entre regiones no pierde eventos ni detiene la transferencia local de r2.

## Variables

| Variable | Valores |
|---|---|
| Región y carga | r1 ~10 pares/s (`--rate 20`), r2 ~3 pares/s (`--rate 6`), 120 s, `pairs` con 400 medidores y prefijo único por fase |
| Credencial | `bridge-r1`, `bridge-r2`, `rogue`, sin credencial |
| Partición | DENY L4 de `voltera-r2` al 7422 del hub global + reinicio de `nats` de r2 (el DENY no corta enlaces ya abiertos), ≥ 120 s con carga en r2 y 4 declaraciones/s en r1 |
| Latencia inter-región | 0 ms. La variante de 80 ms con toxiproxy **no se corrió**: el cluster no tiene toxiproxy y el criterio de 12 s queda sin medir |

## Procedimiento (`experiments/e08-event-mesh/run.sh`)

1. **Reset:** `chaos_heal`, imágenes `billing-sim`, `meter-sim` y `zt-probe`, toolbox con sidecar (`bus-tools`), reinicio de `billing-sim` (su libro vive en memoria; el durable `billing` conserva su posición) y comprobación de `/leafz` (global 2, r1 1, r2 1). Calentamiento de 20 s.
2. **Normal, 120 s:** `meter-sim pairs` en r1 y r2 a la vez. Se espera a que billing tenga todos los matches de la fase.
3. **Negativos, 4 casos x 20 intentos** con `zt-probe` en Jobs con sidecar:
   - `rogue` (credencial válida sin permisos) publica en `energy.matched.r1.*` (ServiceAccount ajeno, `zt-stranger`).
   - Sin credencial publica (mismo ServiceAccount).
   - `bridge-r2` comprometido (SA `mesh-bridge` de r2) publica en `energy.matched.r1.*`.
   - `energy-transfer` de r2, con la única credencial NATS de su namespace (`bridge-r2`), suscribe `energy.matched.>`. `energy-transfer` no tiene usuario NATS propio.
4. **Partición** (ver Variables). Un muestreador pide cada segundo `/leafz` del global y de r2 y el total de `device-sim` r2, desde una toolbox. Se restaura con `chaos_heal` y se espera el drenado.

## Medición

- **Entrega:** conjunto de `matchId` de `matches-db` (vendedor o comprador con el prefijo de la fase) contra `GET /state?detail=1` de billing-sim (`byMatchId`). Pérdida = ids que billing no tiene; duplicados = ids con `count > 1`.
- **Latencia:** billing-sim guarda por match, en la primera entrega, `receivedAt`, `emitLagMs` (`receivedAt - emittedAt`: Kafka, mesh-bridge, NATS, leafnode) y `e2eLagMs` (`receivedAt - occurredAt`). Percentiles por región solo de la fase normal. Cambio en el servicio: `internal/billingsim`, con tests.
- **Autorización:** `zt-probe` clasifica cada intento (`acl`: Permissions Violation; `authn`: Authorization Violation). Además, `last_seq` del stream `BILLING_MATCHED` (`/jsz`) y el total de billing antes y después: 0 mensajes nuevos. Log de `nats-server` de la fase en `nats_server_violations.txt`.
- **Partición:** `leafz_timeline.json`, crecimiento de `device-sim` r2 durante el corte, lag del grupo `mesh-bridge` de r2 (`kafka_consumergroup_lag_sum`), y entrega tras restaurar.
- **Transferencia local de r2:** matches de r2 creados durante el corte con margen de 30 s (los últimos 30 s pueden seguir en el pipeline de Kafka/KEDA) presentes en `device-sim` r2 en el instante de restaurar (`local_ratio`).

## Criterio

Ver `criteria.yaml`. Es el de `03-experiments.md` (E8): 100 % de r1 y r2 en billing, p99 ≤ 10 s, 80/80 rechazos sin llegar a billing y sin falsos rechazos del flujo legítimo, transferencia local de r2 al 100 % durante el corte, pérdida 0 y drenado ≤ 60 s tras restaurar, duplicados 0. Añadidos: `match_ratio ≥ 0.9` (sanidad de la carga), lag del bridge ≥ 20 durante el corte (retiene) y de vuelta a 0.

## Evidencia

`billing_latency.csv`, `authz_attempts.csv`, `leafz_timeline.json` (y `.csv`), `leafz_capture_partition.json` (captura de `/leafz` del global con el corte activo), `nats_server_violations.txt`, `logs/nats-server-*.log`, `logs/mesh-bridge-*.log`, `metrics.json`, `result.json`, `figures/`.

<!-- RESULT -->
