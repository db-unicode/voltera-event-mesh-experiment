# E7. Pérdida de un mensaje de posición sin expulsar al medidor (TTL)

```sh
mise run exp:run e07
```

## Hipótesis

Con TTL = 2,5 ciclos, perder el mensaje de un ciclo no expulsa a ningún medidor de la vista; si se pierden tres ciclos
seguidos la key vence a `TTL` del último refresco; y si se pierden las keys, se repueblan en un ciclo.

## Variables

- M = 200 medidores (`meter-sim --profile steady --meters 200 --period 10`), prefijo único `e07-<ts>-`, región r1.
- Corrida rápida: P = 10 s, TTL = 25 s. El TTL del proyector se fija con `kubectl set env deploy/market-projector VIEW_TTL_S=25`
  y se restaura a `$VIEW_TTL_S` (150) al terminar, también si la corrida falla (trap).
- `declaration-gateway` se fija en 1 réplica con `paused-replicas` (20 decl/s < `C_POD`) para evitar arranques en frío de
  KEDA a mitad de la corrida; se reanuda al final.
- La corrida a escala real (P = 60 s, TTL = 150 s) queda para la ronda de 3 repeticiones.

## Procedimiento

Un solo Job `steady` corre toda la prueba. Un muestreador dentro del pod de Redis (`redis-cli EVAL` cada segundo, un
`kubectl exec` de larga duración) guarda, por segundo: nº de keys del prefijo, PTTL mínimo y, por key, PTTL y `projectedAt`.

| Fase | Inyección | Qué se mide |
|---|---|---|
| 7a | failpoint `drop_position=drop` en el proyector durante 9,5 s (< P: ningún medidor pierde dos mensajes seguidos) | `min(keys)`, TTL mínimo, medidores con mensaje descartado (logs `dropped` por `meterId`) |
| 7b | `drop` durante 30 s (3 ciclos) | por key: desaparición observada - (`projectedAt` + TTL); tiempo hasta `keys == M` tras apagar el failpoint |
| 7c | borrado de las keys del prefijo (Lua `KEYS` + `DEL`; no FLUSHALL) | tiempo hasta `keys == M` |

El failpoint se activa por un `kubectl port-forward` propio al pod del proyector (puerto 19850) para que el inicio y el fin
de la ventana tengan latencia de milisegundos. Los instantes de cada fase se toman del reloj de Redis (`TIME`), el mismo
reloj que las muestras; llevan ~0,1-0,2 s de retraso por el `kubectl exec`.

## Criterio

Ver `criteria.yaml` (13 criterios). Resumen: 7a `min(keys) == 200` y TTL mínimo > 0; 7b las 200 keys vencen con error de
desaparición en [-2, +2] s y reaparecen en <= 15 s (P + 5); 7c `keys == 200` en <= 15 s. Además, 7a debe haber perdido el
mensaje de >= 180 medidores y de ninguno dos veces (valida la inyección).

## Evidencia

`raw/redis_keys.csv` (ts, keys, min_ttl_s, drop_window) y su PNG en `figures/`, `raw/redis_watch.txt` (muestras crudas),
`data/expiry_7b.csv` (una fila por key), `data/drops.csv`, `data/markers.csv`, `metrics.json`, `result.json`.

## Notas

- `view_writes_total{result="expired"}` del enunciado no existe: `view.Store` solo cuenta `applied` y `stale`
  (el vencimiento lo hace Redis). La expiración se mide en las muestras de Redis.
- La resolución del muestreo es 1 s; el criterio de +-2 s del enunciado la cubre (error esperado entre 0 y ~1,2 s).
