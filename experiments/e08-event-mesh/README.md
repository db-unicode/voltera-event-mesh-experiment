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

## Desviaciones

- Sin variante de 80 ms (sin toxiproxy): la latencia inter-región es 0 ms y el criterio de 12 s no se mide.
- Una sola repetición (r1 en `final-run-18789b0.tsv`), no 3.

## Evidencia

`billing_latency.csv`, `authz_attempts.csv`, `leafz_timeline.json` (y `.csv`), `leafz_capture_partition.json` (captura de `/leafz` del global con el corte activo), `nats_server_violations.txt`, `logs/nats-server-*.log`, `logs/mesh-bridge-*.log`, `metrics.json`, `result.json`, `figures/`.

## Resultado

Corrida final `20261017T073700Z-18789b0-dirty-r1` (`experiments/results/final/e08-event-mesh/`, ronda `final-run-18789b0.tsv`): 28/28 criterios en PASS. La marca `-dirty` se debe solo a la evidencia promovida a `results/final` durante la corrida; el código es el del commit indicado.

| Criterio | Observado | Umbral | Veredicto |
|---|---|---|---|
| r1 normal - todo match de r1 llega a billing (matches sin entregar) | 0 | eq 0 | PASS |
| r1 normal - la carga produjo matches (ratio matches/pares enviados) | 0,999 (1199 matches de 1200 pares) | ge 0,9 | PASS |
| r2 normal - todo match de r2 llega a billing (matches sin entregar) | 0 | eq 0 | PASS |
| r2 normal - la carga produjo matches (ratio matches/pares enviados) | 1 (360 de 360) | ge 0,9 | PASS |
| r1 normal - p99 de receivedAt - emittedAt (s) | 0,011320 | le 10 | PASS |
| r2 normal - p99 de receivedAt - emittedAt (s) | 0,007177 | le 10 | PASS |
| r1 normal - p99 de receivedAt - occurredAt (s) | 0,030374 | le 10 | PASS |
| r2 normal - p99 de receivedAt - occurredAt (s) | 0,017111 | le 10 | PASS |
| Efectos duplicados en billing en todas las fases | 0 | eq 0 | PASS |
| Falsos rechazos del flujo legítimo (`mesh_publish_rejected_total` en la fase normal) | 0 | eq 0 | PASS |
| Intentos negativos ejecutados (4 casos x 20) | 80 | eq 80 | PASS |
| Intentos rechazados (80/80) | 80 | eq 80 | PASS |
| Rechazados por autorización (acl o authn), no por red | 80 | eq 80 | PASS |
| Mensajes nuevos en el stream BILLING_MATCHED durante los negativos (`stream_seq_delta`) | 0 | eq 0 | PASS |
| Entregas nuevas en billing durante los negativos (`billing_total_delta`) | 0 | eq 0 | PASS |
| Muestras de /leafz con el enlace caído (global=1, r2=0) | 119 (de 159) | ge 3 | PASS |
| Duración del corte (s) | 135 | ge 120 | PASS |
| Transferencia local de r2 durante el corte (ratio) | 1 | eq 1 | PASS |
| device-sim r2 crece durante el corte (comandos nuevos) | 360 | ge 50 | PASS |
| Lag del grupo mesh-bridge de r2 durante el corte (máximo) | 313 | ge 20 | PASS |
| Lag del grupo mesh-bridge de r2 al final | 0 | eq 0 | PASS |
| Matches de r2 creados en la partición que no llegan a billing | 0 | eq 0 | PASS |
| Matches de r2 creados durante el corte | 360 | ge 100 | PASS |
| Drenado de r2 tras restaurar el enlace (s) | 1,73 | le 60 | PASS |
| r1 sigue entregando durante el corte (matches sin entregar) | 0 | eq 0 | PASS |
| p99 de r1 durante el corte (s) | 3,437 | le 10 | PASS |
| Efectos duplicados en device-sim r2 de los matches de la partición | 0 | eq 0 | PASS |
| Leafnodes del hub al final (2) y de r2 (1) | 2 | eq 2 | PASS |

Notas de la corrida:

- Normal: r1 1199 matches, todos entregados (1200 pares enviados); p50 / p95 / p99 de `receivedAt - emittedAt` = 2,8 / 8,5 / 11,3 ms. r2 360/360, 1,9 / 4,9 / 7,2 ms.
- Negativos: 80/80 rechazados (`bridge-r2`, `nocred`, `rogue` y `sub`, 20 intentos cada uno); `last_seq` del stream +0 y billing +0.
- Partición: 135 s con el enlace caído (119 de 159 muestras de `/leafz` con global=1 y r2=0). device-sim de r2 sumó 360 comandos durante el corte (transferencia local 100 %). El grupo `mesh-bridge` de r2 llegó a 313 mensajes de lag y volvió a 0. Tras restaurar: 360/360 matches de r2 en billing, drenado en 1,73 s, latencia máxima de un match retenido 119,24 s. r1 durante el corte: 240/240, p99 3,44 s.

## Fila de la tabla final

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| **E8 Event mesh 2 regiones:** matches de R1 y R2 a Facturación; publicadores no autorizados; partición de 120 s. **Patrones:** Event Mesh, Zero Trust en el bus. | Distribución real entre regiones con autorización y tolerancia a partición. | Entrega por región, p99 de latencia, rechazos, transferencia local durante el corte. | 100 % entregado; p99 <= 10 s; 80/80 rechazos; pérdida 0 tras reconectar; R2 local al 100 %. | r1 1199 matches (1199/1199 en billing) y r2 360/360; p99 11,3 / 7,2 ms (emisión -> recepción); 80/80 rechazos; corte de 135 s: pérdida 0, drenado 1,73 s, r2 local 100 %; PASS 28/28 (`experiments/results/final/e08-event-mesh/`, corrida `18789b0-dirty-r1`) |
