# Voltera event mesh experiment

Experimento universitario de arquitectura orientada a eventos para Voltera, un motor de emparejamiento P2P de energía. Mide con spikes y experimentos reproducibles:

- Cola durable (RabbitMQ) frente a topic con log retenido (Kafka).
- Event mesh entre dos regiones (NATS).
- Autoescalado por cola y por lag (KEDA).
- CQRS con outbox e idempotencia.
- Zero Trust con Istio (mTLS STRICT).

Todo corre en un cluster kind de un nodo, igual en Linux y macOS (amd64 y arm64).

Flujo: medidores → RabbitMQ → declaration-gateway (KEDA) → positions-db + outbox → RabbitMQ → market-projector → Redis (vista CQRS) → matching-engine → matches-db + outbox → Kafka → energy-transfer (KEDA) → device-sim; Kafka → mesh-bridge → NATS (event mesh) → billing-sim global.

## Requisitos

- Docker en ejecución, con al menos 10 GiB de RAM asignados (12 GiB recomendados en macOS).
- [mise](https://mise.jdx.dev). Instala y fija go, kind, kubectl, helm, istioctl, golangci-lint, shellcheck, kubeconform, jq y natscli.
- Linux: `fs.inotify.max_user_instances >= 512` y `max_user_watches >= 524288` (`mise run doctor` avisa).

## Levantar todo desde cero

Antes: `mise install` y `mise run doctor`. Después, en este orden:

```sh
mise run cluster:up
mise run platform:up
mise run base:up
mise run brokers:kafka
mise run brokers:rabbitmq
mise run data:up r1
mise run data:up r2
mise run mesh:up
mise run apps:write-side all
mise run apps:match-side all
```

- `data:up` acepta `r1` o `r2` (por defecto `r1`). `apps:write-side` y `apps:match-side` aceptan `r1`, `r2` o `all` (por defecto `all`). El resto no recibe argumentos.
- `base:up` crea los namespaces `voltera-infra`, `voltera-r1`, `voltera-r2` y `voltera-global`. Va antes que todo lo demás.
- `apps:write-side` requiere el Secret `rabbitmq-users` (`brokers:rabbitmq`) y, por región, `positions-db` y `market-view` (`data:up`). Si falta alguno, avisa y no aplica el overlay de esa región.
- `mesh:up` crea el stream `BILLING_MATCHED` que consume billing-sim, desplegado por `apps:match-side`.
- Validación: `mise run spike:s00` imprime `PASS` o `FAIL` y deja la evidencia en `experiments/results/s00/<UTC>-<sha>/`.

## Tasks

Lista completa (`mise tasks ls`). Los spikes enlazan a su README.

### Cluster y plataforma

| Task | Qué hace |
|---|---|
| `doctor` | Verifica herramientas, Docker activo, RAM de Docker y límites inotify |
| `cluster:up` | Crea el cluster kind `voltera` (idempotente) |
| `cluster:down` | Elimina el cluster kind `voltera` |
| `platform:up` | Instala Istio, KEDA, observabilidad y metrics-server (idempotente) |
| `base:up` | Crea los namespaces del experimento (idempotente) |

### Brokers, datos y mesh

| Task | Qué hace |
|---|---|
| `brokers:kafka` | Despliega Kafka (KRaft 1 nodo), topics y kafka-exporter en `voltera-infra` (idempotente) |
| `brokers:rabbitmq` | Despliega RabbitMQ 4.3.6 (colas durables, usuarios y definiciones) en `voltera-infra` (idempotente) |
| `data:up [r1\|r2]` | Despliega Postgres y Redis por servicio de una región (idempotente; por defecto `r1`). Ver [deploy/data/README.md](deploy/data/README.md) |
| `data:test` | Tests de integración de `internal/` contra positions-db y market-view (port-forward temporal) |
| `mesh:up` | Despliega los 3 nodos NATS (r1, r2, global) y el stream/consumer de Facturación (idempotente) |

### Aplicaciones

| Task | Qué hace |
|---|---|
| `images:build [nombre...]` | Construye `cmd/<nombre>` como `voltera/<nombre>:dev` y lo carga en kind (sin args: todos) |
| `apps:write-side [r1\|r2\|all]` | Construye y despliega el lado de escritura (meter-sim, declaration-gateway, price-feed-simulator, market-projector) (idempotente). Ver [WRITE-SIDE.md](deploy/apps/WRITE-SIDE.md) |
| `apps:match-side [r1\|r2\|all]` | Construye y despliega el lado de lectura (matching-engine, energy-transfer, device-sim, mesh-bridge) y billing-sim (idempotente). Ver [deploy/apps/README.md](deploy/apps/README.md) |

### Spikes

| Task | Qué hace |
|---|---|
| `spike:s00` | Spike S0: valida la plataforma base (PASS/FAIL) ([detalle](experiments/spikes/s00-platform/README.md)) |
| `spike:s01` | Spike S1: Kafka produce/consume/replay con sidecar Istio (PASS/FAIL) ([detalle](experiments/spikes/s01-kafka/README.md)) |
| `spike:s02` | Spike S2: lag de Kafka en Prometheus con kafka-exporter (PASS/FAIL) ([detalle](experiments/spikes/s02-kafka-lag/README.md)) |
| `spike:s03` | Spike S3: KEDA con trigger Kafka escala 0 -> N -> 0 (PASS/FAIL) ([detalle](experiments/spikes/s03-keda-kafka/README.md)) |
| `spike:s04` | Spike S4: RabbitMQ con colas durables y profundidad en Prometheus (PASS/FAIL) ([detalle](experiments/spikes/s04-rabbitmq/README.md)) |
| `spike:s05` | Spike S5: KEDA escala declaration-gateway con la cola RabbitMQ, 0 -> N -> 0 (PASS/FAIL) ([detalle](experiments/spikes/s05-keda-rabbit/README.md)) |
| `spike:s06` | Spike S6: outbox transaccional con relay Go y crashes (PASS/FAIL) ([detalle](experiments/spikes/s06-outbox/README.md)) |
| `spike:s07` | Spike S7: vista Redis con TTL, versión y recuperación (PASS/FAIL) ([detalle](experiments/spikes/s07-view/README.md)) |
| `spike:s08` | Spike S8: event mesh NATS entre dos regiones con autorización (PASS/FAIL) ([detalle](experiments/spikes/s08-nats-mesh/README.md)) |
| `spike:s12` | Spike S12: primitivas de caos con kubectl (kill, down/up, KEDA, failpoint, partición) (PASS/FAIL) ([detalle](experiments/spikes/s12-chaos/README.md)) |
| `spike:s13` | Spike S13: métricas Go raspadas por el Prometheus de Istio (PASS/FAIL) ([detalle](experiments/spikes/s13-metrics/README.md)) |
| `spike:s15` | Spike S15: lado de lectura r1/r2 (matching -> Kafka -> transfer/device y bridge -> NATS -> billing) (PASS/FAIL) ([detalle](experiments/spikes/s15-match-side/README.md)) |
| `spike:s16` | Spike S16: lado de escritura en r1 (meter-sim, ACL del gateway, proyector, precio, KEDA) (PASS/FAIL) ([detalle](experiments/spikes/s16-write-side/README.md)) |

### Experimentos

| Task | Qué hace |
|---|---|
| `exp:run <id> [rep]` | Corre un experimento (compila expctl; evidencia en `experiments/results/`). Ver [experiments/README.md](experiments/README.md) |
| `exp:e00` | Experimento E00: humo del arnés de experimentos (PASS/FAIL) ([detalle](experiments/e00-harness/README.md)) |
| `exp:report` | Genera `experiments/results/summary.md` con la última corrida de cada experimento |

### Calidad

| Task | Qué hace |
|---|---|
| `lint` | go vet, golangci-lint, shellcheck y kubeconform |
| `test` | Tests de Go con detector de carreras |

## Servicios

| Servicio | Contexto acotado | Consume | Publica | Datos propios | Escalado |
|---|---|---|---|---|---|
| `meter-sim` | Simulador de medidores (`internal/metersim`) | Flags y `METERSIM_*` | `meter-declarations.x` (RabbitMQ) | Ninguno | 0; `kubectl scale` para carga continua o Jobs para corridas acotadas |
| `declaration-gateway` | Medición (`internal/metering`, ACL en `acl/`) | `meter-declarations` (RabbitMQ) | `energy-positions.x` (RabbitMQ) vía outbox | `positions-db` (Postgres) y `outbox` | 0 a 10, KEDA por profundidad de `meter-declarations` |
| `price-feed-simulator` | Precios (`internal/pricing`) | Reloj | `kwh-prices.x` (RabbitMQ) | Ninguno (sin outbox) | 1 |
| `market-projector` | Lado de lectura CQRS (`internal/market/projection`) | `energy-positions` y `kwh-prices` (RabbitMQ) | Redis `market-view` | `market-view` (Redis, vista) | 1 |
| `matching-engine` | Emparejamiento (`internal/market/matching`) | `market-view` (Redis, solo lectura) y ConfigMap `matching-rules` | `<region>.energy-matched` (Kafka) vía outbox | `matches-db` (Postgres) y `outbox` | 1 |
| `energy-transfer` | Transferencia (`internal/transfer`) | `<region>.energy-matched` (Kafka, grupo `energy-transfer`) | Comando HTTP firmado a device-sim | `transfer-db` (Postgres) | 0 a 6, KEDA por lag de Kafka (6 = particiones del topic) |
| `device-sim` | Dispositivo simulado (`internal/devicesim`) | `POST /commands` (HTTP) | Ninguno | Estado en memoria (`GET /state`) | 1 |
| `mesh-bridge` | Puente al mesh (`internal/meshbridge`) | `<region>.energy-matched` (Kafka, grupo `mesh-bridge`) | NATS regional `energy.matched.<region>.<matchId>` | Ninguno | 1 |
| `billing-sim` (global) | Facturación (`internal/billingsim`) | Durable `billing` de `BILLING_MATCHED` (NATS) | Ninguno | Estado en memoria (`GET /state`) | 1 |

Paquetes compartidos: `internal/events` (sobre común de los eventos), `internal/outbox`, `internal/inbox`, `internal/view` (vista Redis) e `internal/broker/` (`amqpx`, `kafkax`, `natsx`). Detalle de los servicios de escritura en [WRITE-SIDE.md](deploy/apps/WRITE-SIDE.md) y de los de lectura en [deploy/apps/README.md](deploy/apps/README.md).

## Estructura

| Ruta | Contenido |
|---|---|
| `cmd/` | Un `main` por servicio, probes de los spikes (`*-probe`) y `expctl` |
| `internal/` | Código Go por contexto (ver tabla de servicios) y paquetes compartidos |
| `deploy/kind/` | Configuración del cluster kind |
| `deploy/platform/` | Istio (operador), KEDA, observabilidad (Prometheus, Kiali, Grafana, Jaeger) y metrics-server |
| `deploy/base/` | Namespaces del experimento |
| `deploy/brokers/` | Kafka (`kafka/`) y RabbitMQ (`rabbitmq/`) en `voltera-infra` |
| `deploy/data/` | Bases `postgres/` y `redis/`, e instancias por región en `r1/` y `r2/` ([detalle](deploy/data/README.md)) |
| `deploy/mesh/nats/` | Nodos `r1/`, `r2/` y `global/`, y `setup/` (stream y consumer de Facturación) |
| `deploy/apps/` | `base/<servicio>/`, overlays `r1/` y `r2/` (`write-side`, `match-side`) y `global/billing/` ([detalle](deploy/apps/README.md)) |
| `experiments/spikes/` | Spikes `sNN-<nombre>/`, cada uno con `run.sh` y README |
| `experiments/e00-harness/` | Experimento E00 (arnés); los experimentos siguen el patrón `eNN-<nombre>/` |
| `experiments/lib/` | Librería shell: chaos, Kafka y AMQP de los spikes, lock, snapshot del entorno y expctl |
| `experiments/results/final/` | Corridas entregadas. El resto de `experiments/results/` no se versiona |
| `experiments/run.sh`, `params.env` | Runner de experimentos y parámetros globales ([detalle](experiments/README.md)) |
| `mise-tasks/` | Tasks de mise como archivos (`cluster/up` es `cluster:up`) |
| `docs/adr/` | Decisiones de arquitectura (ver [ADR](#adr)) |

## ADR

- [0001. Registro de decisiones de arquitectura](docs/adr/0001-registro-de-decisiones.md)
- [0002. Cluster local con kind](docs/adr/0002-cluster-kind.md)
- [0003. Tres brokers con papeles distintos](docs/adr/0003-brokers.md)
- [0004. Regiones como namespaces](docs/adr/0004-regiones-en-namespaces.md)
- [0005. Event mesh NATS: hub con leafnodes](docs/adr/0005-event-mesh-nats.md)

## Convenciones

- Scripts POSIX `sh` (`set -eu`), validados con `shellcheck -s sh`.
- Código e identificadores en inglés; documentación en español.
- Commits `type(scope): <gitmoji> summary`; trunk-based con ramas cortas y merge `--no-ff`.
