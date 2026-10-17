# E5. Idempotencia por duplicados

Con entrega al-menos-una-vez, duplicados y desorden, cada efecto de negocio ocurre una sola vez en el gateway, en
`energy-transfer` y en `billing-sim`.

```sh
mise run exp:run e05
```

## Hipótesis

El gateway (versión en `positions`), `energy-transfer` (inbox por `matchId`) y `billing-sim` (ledger por `matchId`) absorben
copias y reentregas sin duplicar el efecto. Lo que descartan se puede contar y cuadra con las entregas.

## Variables

| Variable | Valor por defecto | Dónde |
|---|---|---|
| Medidores únicos / copias de v5 | `E5_METERS=300` x `E5_K=6` | (a) `meter-sim --profile dup --version-base 5` |
| Medidores con v3 posterior / copias | `E5_METERS_V3=150` x `E5_K3=4` | (a) `--version-base 3` (desorden: v5 y luego v3) |
| Réplicas fijas | gateway 3, energy-transfer 2 (KEDA en pausa) | los contadores por pod no se pierden |
| Matches reenviados / copias | los de una carga `pairs` de 20 decl/s x 10 s, `E5_RESEND_K=5` | (b), (d) |
| Failpoint | `before_ack=crash-after:60` (gateway), `crash-after:40` (energy-transfer), hasta 3 crashes cada uno | (c) con carga `pairs` de 20 decl/s x 30 s |

## Procedimiento

0. Prefijos únicos por corrida (`e05-<ddhhmmss>-a|b|c-`). KEDA del gateway y de `energy-transfer` queda en pausa con réplicas
   fijas y se restaura al terminar.
1. **Generación (b)**: carga `pairs`; se espera a que los matches estén en BD, con transferencia `SENT` y un efecto en device-sim.
2. **(a) gateway**: `dup` con 300 medidores x 6 copias de v5 y después 150 x 4 de v3. Se suman los contadores `/metrics` de los pods
   antes y después. Se comprueba la outbox de `positions-db`, `positions` y la vista Redis (`_v == 5`).
3. **(c) redelivery real**: `before_ack=crash-after:N` en el gateway y en `energy-transfer` mientras corre una carga `pairs`; un bucle
   vuelve a armar el failpoint tras cada crash. Los pods mueren después de aplicar y antes de ackear.
4. **(b) energy-transfer**: pasados 135 s desde que se procesaron los matches de (1), un Job con `kafka-console-producer` reenvía cada
   `EnergyMatched` (mismo sobre y `eventId`, leído de la outbox) 5 veces a `r1.energy-matched`.
5. **(d) billing-sim**: el reenvío de (4) ocurre fuera de la `duplicate_window` de 120 s del stream `BILLING_MATCHED`, así que
   `mesh-bridge` lo publica a NATS y el stream acepta una copia por `matchId` (las otras cuatro caen dentro de la ventana del reenvío).

## Métricas y evidencia

Corrida: `experiments/results/e05-duplicates/<UTC>-<sha>-r<rep>/`.

| Archivo | Contenido |
|---|---|
| `metrics.json` | todas las cifras que evalúa `criteria.yaml` (las comparaciones con un conteo medido van como diferencia == 0) |
| `effects.json` | efectos finales: device-sim, transfers, outbox, billing |
| `dup_counters.json` | entregas, aplicados, `stale`, `error`, balance del gateway; duplicados descartados por `energy-transfer`; duplicados de billing |
| `reconciliation.csv` | por `eventId` (gateway) o `matchId` (transfer/billing): entregado N veces, efectos observados, efectos esperados |
| `raw/` | salida de `meter-sim`, listas de ids, estados de device-sim y billing-sim antes/después, `crashes-*.csv` |

Balance del gateway: `events_consumed_total{channel="meter-declarations"}` por resultado. Una declaración con versión igual o menor
a la guardada cuenta como `stale` (el gateway no distingue duplicado exacto de versión vieja); por eso
`events_duplicates_dropped_total` del gateway queda en 0 y el balance es `ok + stale + error == entregas`.

En `energy-transfer` todas las copias reenviadas son duplicados (el original se aplicó antes), así que el contador sube `N x k`, no `N x (k-1)`.
En (c) no se usan contadores: el crash reinicia el proceso y pierde lo que no alcanzó a scrapearse; se comprueban solo efectos.

## Criterio

Ver `criteria.yaml` (28 criterios). Resumen: 1 fila de outbox por (medidor, versión) aceptada; `aplicados + stale + error == entregas`;
Redis conserva v5; device-sim aplica cada `matchId` exactamente una vez; transfers e inbox sin duplicados; duplicados descartados
== `N x k`; con crashes, las mismas igualdades; billing-sim registra 2 entregas por `matchId` reenviado, `distinct` no cambia y
`duplicates` sube `N`.

## Resultado

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| E5 Duplicados | Cada efecto ocurre una vez con duplicados, desorden y redelivery | efectos vs entregas, descartes, v5 en Redis | igualdades exactas (ver arriba) | _pendiente_ |
