# E6. Outbox: caída entre commit y publicación

Si `matching-engine` muere después de confirmar un match y antes de publicarlo, el evento sale igual al reiniciar. Sin outbox
(control) se pierde.

```sh
mise run exp:run e06
```

Requiere la imagen `voltera/matching-engine:dev` con `OUTBOX_MODE` (`mise run images:build matching-engine`); `exp_reset` reinicia
el deployment una vez si el pod no declara `outboxMode` en `/debug/state`.

## Hipótesis

Con outbox transaccional, match y evento se confirman juntos y el relay publica lo pendiente tras un crash (al-menos-una-vez).
Con escritura dual ingenua (commit sin outbox y publicación directa) la misma caída deja matches en BD sin evento.

## Variables

| Variable | Valor | Nota |
|---|---|---|
| Carga | `meter-sim --profile pairs`, 20 pares/s (40 decl/s) x 120 s, medidores únicos | 2400 matches esperados |
| Fase 1 | `after_commit_before_publish=crash-after:200`, modo `outbox` | hasta 2 crashes (se re-arma tras cada reinicio) |
| Fase 2 | `after_publish_before_mark=crash-after:200`, modo `outbox` | publica y muere antes de marcar: duplicados en el broker |
| Fase 3 (control) | `OUTBOX_MODE=direct`, `after_commit_before_publish=crash-after:200`, 60 s | 1 crash |

Failpoints por `POST /admin/failpoint` (el proceso reiniciado nace sin failpoints; `FAILPOINT_STATE_DIR` además evita que un
`crash-after` vuelva a disparar tras el reinicio del contenedor).

### Control: `OUTBOX_MODE`

`matching-engine` lee `OUTBOX_MODE=outbox|direct` (por defecto `outbox`; cualquier otro valor impide arrancar). En `direct`,
`Store.Persist` inserta `matches` y `matched_positions` pero no la fila de outbox, y `Engine.publishDirect` publica cada
match a Kafka justo después del commit, con el failpoint `after_commit_before_publish` entre ambos pasos. El experimento lo activa con
`kubectl set env deploy/matching-engine OUTBOX_MODE=direct` solo durante la fase 3 y lo deshace al terminar (también si falla).
`/debug/state` expone `outboxMode`.

## Procedimiento por fase

1. Un muestreador (1 Hz) anota `ts,pending,matches,restarts` en `raw/pN-samples.csv` (`pending` = filas con `published_at IS NULL`).
2. Se arma el failpoint; un bucle lo vuelve a armar tras cada crash y anota en `raw/pN-crashes.csv` el fin del contenedor caído.
3. Carga `pairs` con prefijo único `e06-<ddhhmmss>-pN-`; se espera a que los matches se estabilicen, a que la outbox quede vacía y
   (con outbox) a que device-sim aplique todos.
4. Un Job con `kafka-console-consumer --from-beginning` lee `r1.energy-matched` y se queda con los registros del prefijo
   (`raw/pN-kafka_event_ids.txt`). Si falta algún match se repite la lectura (se unen las lecturas).
5. Métricas: huérfanos (`matches` sin fila de outbox con `key = match_id`), pendientes finales, `comm` entre `matches_ids` y
   `kafka_event_ids` (pérdida y sobrantes), `eventId` repetidos en el topic, efectos en device-sim (`byMatchId`), duplicados de
   transfers y recuperación por crash (del fin del contenedor a la primera muestra con `pending == 0`).

## Evidencia

`experiments/results/e06-outbox/<UTC>-<sha>-r<rep>/`: `metrics.json`, `outbox_before_after.csv` (una fila por fase),
`kafka_event_ids-pN.txt`, `matches_ids-pN.txt`, `raw/pN-samples.csv`, `raw/pN-crashes.csv`, `raw/pN-recovery.json`,
`raw/pN-lost_ids.txt` (el diff `comm -23` de BD contra topic) y `figures/pN-samples.png` (`pending`, `matches` y `restarts` en el tiempo).

## Criterio

Ver `criteria.yaml`. Fases 1 y 2: matches sin outbox = 0; pendientes finales = 0; pérdida = 0 (y 0 sobrantes en el topic);
efecto único en device-sim; recuperación <= 30 s por crash; en la fase 2 al menos un `eventId` repetido en el broker. Fase 3:
pérdida >= 1.

## Resultado

Corrida final `20261017T072649Z-18789b0-dirty-r1` (`experiments/results/final/e06-outbox/`, ronda `final-run-18789b0.tsv`): 26/26 criterios en PASS. La marca `-dirty` se debe solo a la evidencia promovida a `results/final` durante la corrida; el código es el del commit indicado.

- Fase 1 (outbox, `after_commit_before_publish`): 2400 matches de 2400 esperados, 2 crashes, 0 huérfanos, 0 pendientes, 0 pérdidas, 0 sobrantes en el topic; 14 eventos repetidos en el topic con efecto único en device-sim; recuperación de 11 s.
- Fase 2 (outbox, `after_publish_before_mark`): 2400 de 2400, 2 crashes, 0 huérfanos, 0 pendientes, 0 pérdidas; 16 eventos repetidos en el topic con efecto único; recuperación de 11 s.
- Control (`direct`, `after_commit_before_publish`): 1200 de 1200 matches, 1200 sin fila de outbox; 1192 `matchId` distintos en el topic (8 perdidos) y 8 efectos ausentes en device-sim; recuperación de 1 s.

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| E6 Outbox | Ningún evento se pierde por caída entre commit y publish; sin outbox sí | huérfanos, pendientes finales, pérdida, recuperación | 0 / 0 / 0 / <= 30 s; control con pérdida >= 1 | PASS 26/26: fases 1 y 2 con 2400/2400 matches, 0 huérfanos, 0 pendientes, 0 pérdidas y efecto único en device-sim (14 y 16 eventos repetidos en el topic), recuperación 11 s; control `direct`: 1200 matches sin fila de outbox y 8 perdidos, recuperación 1 s · `experiments/results/final/e06-outbox/` (corrida `18789b0-dirty-r1`) |
