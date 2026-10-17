# S6. Outbox transaccional con relay en Go

Estado de negocio y evento se confirman en una transacción (`outbox.Append`); un relay publica después con
entrega al-menos-una-vez (`internal/outbox`).

## Ejecutar

```sh
mise run data:up && mise run spike:s06
```

## Montaje

- Postgres propio del spike en el namespace `s06-outbox` (`s06-db`, misma base que `deploy/data/postgres`). No usa `positions-db`, así que no deja nada ahí; se borra con el namespace.
- Job `outbox-probe` (`cmd/outbox-probe`): siembra 1000 filas de negocio, cada una con su evento en la misma transacción (con 2 ms entre filas, para que el crash caiga a mitad de la siembra), mientras el relay publica en un `Publisher` que inserta en `sink(event_id, received_at)` sin unique. La siembra es reanudable (`ON CONFLICT DO NOTHING`).
- El Job tiene `FAILPOINT_STATE_DIR=/state` (emptyDir): el contenedor reiniciado no vuelve a morir.

## Escenarios y checks

| Escenario | `FAILPOINTS` | Esperado |
|---|---|---|
| `control` | (ninguno) | 0 reinicios, 1000 distintos, 0 duplicados |
| `commit-publish` | `after_commit_before_publish=crash-after:200` | exit 137, reinicio, `pending = 0`, 1000 distintos, 0 duplicados |
| `publish-mark` | `after_publish_before_mark=crash-after:250` | exit 137, reinicio, `pending = 0`, 1000 distintos, duplicados > 0 |

Las cuentas las hace `run.sh` con `psql` (no confía en el resumen del probe).

## Notas

- `committedAt` del sobre es `outbox.created_at` (`clock_timestamp()`); el relay lo rellena al leer la fila y fija `emittedAt` en cada intento.
- La tabla `outbox` lleva además `aggregate_id` (no estaba en la spec): E6 la usa para cruzar con `matches`.
- Con lotes de 100, un crash antes de marcar deja hasta 100 filas ya publicadas sin marcar; cuántas dependen de cuándo cae el crash. Aquí el relay va al paso de la siembra (lotes pequeños), por eso salen pocos duplicados (1 en la corrida de referencia).
- Un contenedor con exit 137 se reinicia en el mismo pod (`restartPolicy: OnFailure`); `kubectl logs --previous` del contenedor muerto no estuvo disponible en kind al terminar el Job, por eso el log del contenedor nuevo registra `failpoint already fired before restart`.

## Evidencia

`experiments/results/s06/<UTC>-<sha>/`: `result.txt`, `<escenario>.log` (log del probe), `<escenario>.db.json` (conteos), `<escenario>.pod.json` (estado de contenedores: `restartCount`, `lastState`), `checks/`.
