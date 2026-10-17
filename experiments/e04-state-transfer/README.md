# E4. Transferencia de estado: el proyector nunca toca `positions-db`

```sh
mise run exp:run e04
```

## Hipótesis

El evento `PositionDeclared` lleva el estado completo: `market-projector` construye la vista sin acceder a `positions-db`
y sigue proyectando con la base apagada.

## Variables y procedimiento (región r1, ~10 min)

Orden de las pruebas: (2), (1), (3), (4), grafo de Kiali. La (2) va primero para que los intentos rechazados de (1) no
entren en su ventana de conexiones.

1. **(2) Ausencia de conexiones.** 5 min de carga base (3000 medidores / 30 s = 100 decl/s). Cada 15 s, `pg_stat_activity`
   (sin la sesión propia). Al final, conexiones nuevas por origen en la ventana = `sum by (source_workload)(istio_tcp_connections_opened_total{destination_workload="positions-db",reporter="destination"})`
   menos el mismo contador `offset <ventana>` (origen nuevo: desde 0). No se usa `increase()`: una serie que nace dentro de la ventana con
   una sola muestra devuelve 0 y ocultaría un origen ajeno.
   Con el sidecar, `client_addr` es siempre `127.0.0.6`: el cliente se identifica por `application_name`, que el gateway fija
   en su `DATABASE_URL` (`application_name=declaration-gateway`, cambio de una línea en `deploy/apps/base/declaration-gateway/deployment.yaml`).
2. **(1) Política.** `deploy/security/` (task `security:up`, ambas regiones): `AuthorizationPolicy` ALLOW sobre `positions-db`,
   puerto 5432, solo `cluster.local/ns/voltera-<r>/sa/declaration-gateway`. Dos Jobs con `postgres:18.6` y 20 intentos
   `psql ... select 'connected'` con la contraseña real: uno con SA `market-projector` (debe fallar 20/20) y uno con SA
   `declaration-gateway` (control, debe conectar 20/20). El rechazo es de red, no de contraseña: se cuentan los errores
   `authentication`. Contador Envoy `tcp.rbac.denied` del sidecar de `positions-db`, antes y después.
3. **(3) Estática.** `go list -deps ./cmd/market-projector` sin `pgx`, `jackc` ni `lib/pq`; el mismo grep sobre
   `cmd/declaration-gateway` encuentra drivers (control del grep). El Deployment del proyector no tiene variables, `envFrom` ni
   Secrets con `positions`, `postgres`, `DATABASE_URL`, `PG_` o `DSN`.
4. **(4) Autosuficiencia.** `positions-db` a 0 réplicas; `meter-sim --profile direct-positions --meters 500 --rate 0`
   publica 500 `PositionDeclared` (estado completo, mismo sobre que el relay) en `energy-positions.x` saltándose gateway y
   base; se cuenta cada segundo cuántas keys del prefijo hay en Redis. Se restaura a 1 réplica.
5. **Kiali.** Grafo de workloads de `voltera-r1` por la API (`/kiali/api/namespaces/graph`, tráfico TCP de la ventana): sin
   arista `market-projector` -> `positions-db`, con arista `declaration-gateway` -> `positions-db`.

## Herramientas de medición (no son parte del dominio)

- `direct-positions` necesita un usuario RabbitMQ con write sobre `energy-positions.x`: se usa el de `declaration-gateway-r1`
  (único, además de `admin`), copiado en el Secret `e04-direct-rabbitmq` del namespace (`probejob_direct_secret`).
- Imagen `voltera/meter-sim:exp-e03-e04-e07` (los perfiles nuevos no están en `:dev` hasta integrar).

## Criterio

Ver `criteria.yaml`: (1) 20/20 rechazados, 0 errores de autenticación, `rbac denied` >= 20, control 20/20; (2) 0 clientes de
`pg_stat_activity` distintos de `declaration-gateway`, 0 orígenes ajenos en Prometheus, 0 conexiones del proyector;
(3) 0 drivers y 0 variables de BD; (4) 500/500 keys en <= 10 s con la base apagada; Kiali sin la arista.

## Evidencia

`deny.log`, `control.log`, `rbac_stats.json`, `pg_stat_activity.csv`, `istio_tcp.json` (ventana de la prueba 2) e
`istio_tcp_after_deny.json` (incluye al pod de prueba como `source_workload`, nunca como `market-projector`), `go_deps.txt`,
`go_deps_gateway.txt`, `projector_env_names.json`, `kiali_graph.json`, `raw/redis_direct.csv`, `metrics.json`, `result.json`.
La captura visual de Kiali no se automatiza (sin navegador): el grafo JSON es la evidencia comprobable; para la imagen,
`kubectl -n istio-system port-forward svc/kiali 20001:20001`, namespace `voltera-r1`, grafo Workload, mostrar tráfico TCP.

## Notas

- `database/sql` (stdlib, no es un driver) aparece en `go list -deps ./cmd/market-projector` porque
  `prometheus/client_golang/prometheus/collectors` lo importa (DBStatsCollector) y `internal/telemetry` usa ese paquete.
  El criterio se aplica a los drivers (`pgx`, `jackc`, `lib/pq`); el importador de `database/sql` se registra en `metrics.json`
  (`static.stdlib_database_sql_importers`). Sin driver registrado, `database/sql` no puede hablar con Postgres.
- La política no afecta a `kubectl exec` ni a `kubectl port-forward` (entran por el loopback del pod, sin pasar por el filtro
  RBAC): `expctl sql`, los spikes y `mise run data:test` siguen funcionando.

## Resultado (corrida final, `results/final/e04-state-transfer/`)

Evidencia: `experiments/results/final/e04-state-transfer/` (corrida `20261017T071724Z-18789b0-dirty-r1`, ronda `final-run-18789b0.tsv`). 19 de 19 criterios PASS. La marca `-dirty` se debe solo a la evidencia promovida a `results/final` durante la corrida; el código es el del commit indicado.

| Prueba | Observado | Criterio |
|---|---|---|
| (1) intentos de SA `market-projector` rechazados | 20 de 20; 0 errores de autenticación | 20/20 |
| (1) `tcp.rbac.denied` del sidecar de `positions-db` | +20 (`allowed` +20 por el control) | >= 20 |
| (1) control con SA `declaration-gateway` | 20 de 20 conectan | 20/20 |
| (2) `pg_stat_activity`, 21 muestras | un solo cliente: `voltera/declaration-gateway` | = {declaration-gateway} |
| (2) conexiones a `positions-db` por origen (Prometheus) | `declaration-gateway`: 50; `market-projector`: 0 | solo el gateway |
| (3) drivers de Postgres en `go list -deps ./cmd/market-projector` (284 paquetes) | 0 (el gateway, como control: 16) | 0 |
| (3) variables de BD en el Deployment del proyector | 0 | 0 |
| (4) 500 `PositionDeclared` directos con `positions-db` en 0 réplicas | 500 de 500 keys en Redis a 1,299 s | 500/500 en <= 10 s |
| Kiali, grafo de `voltera-r1` | arista `declaration-gateway` -> `positions-db` (1); `market-projector` -> `positions-db` (0) | sin la arista |

Tras la prueba (1), `istio_tcp_after_deny.json` muestra los intentos como `source_workload` = el Job de prueba (20 del rechazado y 20 del control), nunca como `market-projector`: el rechazo ocurre después de que el sidecar de origen abre la conexión mTLS.


## Tabla final

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| E4 Transferencia de estado | El proyector construye la vista sin acceder a `positions-db` y sigue proyectando con la base apagada | Intentos rechazados y `rbac denied`; clientes de `pg_stat_activity` y de `istio_tcp_*`; drivers de BD en el binario; keys en Redis con la base apagada | 20/20 rechazados y denied > 0; solo `declaration-gateway`; 0 drivers; 500/500 en <= 10 s | 20/20 rechazados, denied +20, control 20/20; un cliente (`declaration-gateway`), 0 conexiones del proyector; 0 drivers, 0 variables de BD; 500/500 en 1,299 s; Kiali sin la arista. PASS 19/19 (`experiments/results/final/e04-state-transfer/`, corrida `18789b0-dirty-r1`) |
