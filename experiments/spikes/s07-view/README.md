# S7. Vista Redis con TTL, versión y recuperación

`internal/view`: una key hash por medidor; `Upsert` es un script Lua atómico que aplica solo si la versión entrante es mayor que la guardada y renueva el TTL.

## Ejecutar

```sh
mise run data:up && mise run spike:s07
```

**Ejecuta `FLUSHALL` sobre `market-view`** (r1). La vista es efímera por diseño; no lo corras mientras haya un experimento usándola.

## Qué comprueba (`cmd/view-probe`, Job en `s07-view`, ~25 s)

1. v5 se aplica; v3 después devuelve `stale` y la vista conserva v5.
2. TTL observado = `VIEW_TTL_S` (150 s) ± 1 s tras escribir.
3. Un `stale` no renueva el TTL.
4. Con TTL 10 s, la key desaparece a los 10 ± 2 s (medido con sondeo cada 50 ms).
5. `FLUSHALL` vacía la vista y una nueva declaración la repuebla (la versión empieza de nuevo: sin estado previo no hay con qué comparar).
6. `view_writes_total{result}` cuenta `applied` y `stale`.

El probe copia el Secret `market-view-credentials` desde `voltera-r1` al namespace del spike (el Job corre dentro del mesh y llama a `market-view.voltera-r1` por mTLS).

## Evidencia

`experiments/results/s07/<UTC>-<sha>/`: `result.txt`, `probe.log` (salida completa), `probe-checks.txt`, `checks/`.
