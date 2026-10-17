# Lado de escritura (grupo `write-side`)

`mise run apps:write-side [r1|r2|all]` construye las imágenes, copia las credenciales, aplica `deploy/apps/<región>/write-side/`
y espera los rollouts. Si en la región faltan `positions-db` o `market-view` (hoy r2), avisa y no aplica el overlay (queda listo).

```
meter-sim --(x-vendor)--> meter-declarations --> declaration-gateway --> positions-db + outbox
                                (KEDA)                  |  relay in-process
price-feed-simulator --> kwh-prices ----+          energy-positions
                                        v               v
                                     market-projector --> Redis market-view (pos:<zona>:<medidor>, price:<región>)
```

| Servicio | Réplicas | Entrada | Salida | Usuario RabbitMQ |
|---|---|---|---|---|
| `meter-sim` | 0 (Deployment para carga continua; Jobs para corridas acotadas) | flags / `METERSIM_*` | `meter-declarations.x` | `meter-sim-<r>` |
| `declaration-gateway` | 0 a 10 (KEDA, profundidad de `meter-declarations`) | `meter-declarations` | positions-db + outbox -> `energy-positions.x` | `declaration-gateway-<r>` |
| `price-feed-simulator` | 1 | reloj | `kwh-prices.x` | `price-feed-<r>` |
| `market-projector` | 1 | `energy-positions`, `kwh-prices` | Redis `market-view` | `market-projector-<r>` |

Cada servicio tiene su Secret `<servicio>-rabbitmq` (`AMQP_USER`, `AMQP_PASSWORD`), copiado por la task desde `rabbitmq-users`
de `voltera-infra` con solo su usuario. El gateway además necesita `keda-rabbitmq-uri` (URI del management API con el usuario
`keda`, de solo lectura) para el `TriggerAuthentication` `rabbitmq-http` (patrón de S5).

## meter-sim

Dos fabricantes, cabecera `x-vendor`:

- `acme-v1`: `{"meter":"m-0001","mode":"SELL|BUY","energyWh":4200,"horizonSec":120,"seq":17,"ts":"RFC3339Nano"}`. Sin zona.
- `voltix-2`: `{"id":"m-0002","position":{"type":"surplus|deficit","kwh":3.1},"validity":{"until":"..."},"version":9,"zoneCode":"Z3","measuredAt":"..."}`.

**Zona de acme**: la ACL del gateway la deriva del medidor con `acl.ZoneOf(meterId, ZONES)`: `z-((dígitos finales mod ZONES) + 1)`
(sin dígitos finales, FNV-1a del id). `ZONES` del gateway y `--zones` de meter-sim deben coincidir (4 por defecto). meter-sim
usa la misma función para dar a cada medidor su zona, también en voltix (`zoneCode` = `Z<n>`). Con `--vendor mixed`, la mitad de
los medidores de cada zona usa cada formato.

| Flag (env `METERSIM_*`) | Efecto |
|---|---|
| `--profile steady\|pairs\|spike\|dup\|once\|probe\|direct-positions` | steady: tasa fija recorriendo los medidores (la versión sube en cada vuelta). pairs: por zona, pares SURPLUS/DEFICIT de igual kWh (1 match esperado por par; `expectedPairs` en el resumen). spike: rampa lineal (`--ramp`) a `--rate` y meseta. once: cada medidor una vez. probe y direct-positions: ver *Perfiles de medición*. dup: el lote de `once` (mismos `eventId` y versiones) `--dup-k` veces, mezclado. |
| `--rate` | declaraciones/s. Si falta en steady/pairs/spike: `meters/period`, o 100. En once/dup `0` = sin límite |
| `--duration` | s o `1m30s` (steady, pairs, spike) |
| `--meters`, `--zones`, `--period`, `--seed`, `--dup-k` | medidores (`m-0001`...), zonas, s entre declaraciones de un medidor, semilla, copias de dup |
| `--out` | CSV `eventId,meterId,version,sentAt` |
| `--prefix`, `--vendor`, `--horizon`, `--ramp`, `--workers`, `--version-base` | prefijo de ids (`m-`); `mixed`/`acme-v1`/`voltix-2`/otro valor (header desconocido con cuerpo acme); vigencia (120s); rampa (5s); publicadores (16); primera versión (once/dup: 1; resto: unix time) |

### Perfiles de medición (E3 y E4; no son parte del dominio)

| Perfil | Qué hace | Credenciales |
|---|---|---|
| `probe` | Medidores `probe-*` (`--prefix probe-...`) que declaran `--rate` sondeos/s (1 por defecto) con una versión que no se repite; al confirmarse la publicación, cada sondeo lee `_v` de `pos:<zona>:<medidor>` en `market-view` cada `--poll` (20 ms) hasta ver su versión y anota `visible_at` (`--out`, CSV `eventId,meterId,version,sent_at,confirmed_at,visible_at,sent_us,visible_us,status`). `--read-rate N` añade N lecturas/s de fondo (`HGETALL` de keys `pos:*` al azar; `--read-out`: CSV por segundo con percentiles). `--rate 0` con `--read-rate` = solo lectores | Además de las de AMQP: `REDIS_PASSWORD` del Secret `market-view-credentials` (Redis no tiene usuarios de solo lectura; el perfil solo lee) y `REDIS_ADDR` (`market-view.voltera-<r>:6379`) |
| `direct-positions` | Como `once`, pero publica `PositionDeclared` completos (estado, `committedAt` y `emittedAt` = instante de envío) en `energy-positions.x`, saltándose el gateway y `positions-db` | Usuario RabbitMQ con write sobre `energy-positions.x`: solo `declaration-gateway-<r>` (y `admin`). Los experimentos lo copian al Secret `e04-direct-rabbitmq` |

Con `--out -` / `--read-out -` los CSV salen por stdout con prefijo `PROBE ` / `READS ` (el Job no tiene volumen; ver `experiments/lib/probejob.sh`).

Modelo abierto: un temporizador encola cada declaración en su instante; si la cola de envío (4096) está llena se descarta
(`dropped`); si sale más de 100 ms tarde cuenta `late`. `eventId` = UUIDv5(seed/medidor/versión): `once` y `dup` con la misma
semilla producen los mismos ids. `traceparent` nuevo por mensaje. Termina con un resumen JSON en stdout y `/metrics` en :9090
(`events_published_total`, `meter_sim_late_total`, `meter_sim_dropped_total`, `meter_sim_publish_errors_total`).

## declaration-gateway

- `internal/metering/acl` traduce cada formato a `events.PositionDeclared` (y valida: lado, kWh >= 0, versión >= 1, vigencia
  posterior a la medición y <= 24 h, id `[A-Za-z0-9._-]{1,64}`). Formato desconocido o inválido: `declarations_rejected_total{vendor,reason}`
  (`vendor="unknown"` si no es uno de los dos conocidos) y `events_consumed_total{result="error"}`; nack sin requeue.
- Una transacción: `INSERT ... ON CONFLICT (meter_id) DO UPDATE ... WHERE positions.version < EXCLUDED.version`; si no toca
  filas es duplicado o viejo (`events_stale_dropped_total`, sin outbox); si aplica, `outbox.Append(PositionDeclared, "energy-positions")`.
  El ack va después del commit. El `eventId` de la declaración pasa a ser el del evento (seguimiento de punta a punta).
- `PROCESSING_DELAY_MS` (20) simula el coste de validar/traducir: fija la capacidad de una réplica en ~1/(20 ms + commit)
  ≈ 40-45 msg/s y hace visible el escalado con carga modesta. `0` lo quita.
- `DATABASE_URL` lleva `application_name=declaration-gateway`: con el sidecar `client_addr` es siempre 127.0.0.6 y `pg_stat_activity` solo distingue clientes por ese nombre (E4).
  `positions-db` solo acepta conexiones de este ServiceAccount (`deploy/security/`, `mise run security:up`).
- Failpoints: `before_ack` (tras el commit, antes del ack) y los del relay (`after_commit_before_publish`, `after_publish_before_mark`).
- Al apagarse publica lo pendiente de la outbox (hasta 15 s): con KEDA en 0 réplicas nadie más lo haría. Si RabbitMQ no
  responde, lo pendiente espera al siguiente arranque, que KEDA solo provoca si llegan declaraciones.

## price-feed-simulator

`PRICE_MIN_INTERVAL_S`/`PRICE_MAX_INTERVAL_S` (5/15), `PRICE_BASE` (0,15), `PRICE_JITTER` (0,01: paso uniforme ±jitter, acotado a
base ± 5·jitter), `PRICE_SEED`. `version` = unix ms estrictamente creciente; `validUntil` = ahora + 2 × intervalo máximo.
**Sin outbox ni base de datos**: no hay estado propio que deba confirmarse con el evento; el precio es regenerable y el siguiente
evento (<= 15 s) trae el estado completo con versión mayor, así que un envío perdido se corrige solo.

## market-projector

Aplica con `view.Store.Upsert` (versión = `Envelope.Version`): `pos:<zona>:<medidor>` (TTL `VIEW_TTL_S`, 150) y `price:<región>`
(TTL `PRICE_TTL_S`, 300), según el contrato de la vista. No tiene DSN ni enlaza ningún driver de Postgres
(`internal/market/projection/deps_test.go` y el check (f) de S16). `event_e2e_lag_seconds{stage="committed"}` mide commit en
positions-db -> vista; `view_writes_total{result}` lo cuenta `view.Store`. Failpoint `drop_position`: ack y descarte
(`failpoint_dropped_total`). Limitación: si un medidor cambia de zona, la key de la zona anterior vive hasta su TTL.

## amqpx (`internal/broker/amqpx`)

```go
func ConfigFromEnv(getenv func(string) string) (Config, error)   // AMQP_HOST|PORT|USER|PASSWORD|VHOST (o REGION)
func ConfigFromURI(uri string) (Config, error)

func NewPublisher(cfg Config, opts PublisherOptions) *Publisher  // conecta al primer Send/Ping; reconecta solo
func (p *Publisher) Send(ctx context.Context, o Outgoing) error  // persistente + mandatory; vuelve tras el confirm
func (p *Publisher) Ping(ctx context.Context) error
func (p *Publisher) Close() error
func EnvelopeOutgoing(topic string, env events.Envelope, body []byte) Outgoing // "<topic>.x", key "<topic>", headers traceparent/eventId/type

func NewConsumer(cfg ConsumerConfig) (*Consumer, error)
func (c *Consumer) Run(ctx context.Context, h Handler) error     // ack tras el handler; reconecta
func (c *Consumer) Ready(ctx context.Context) error
type Handler func(ctx context.Context, d Delivery) error          // nil: ack; Invalid(err): nack sin requeue; otro: nack + requeue con backoff

// internal/broker/amqpx/outboxpub
func New(p *amqpx.Publisher) *Publisher                           // implementa outbox.Publisher
```

`outboxpub` está aparte porque `internal/outbox` importa pgx y `market-projector` no puede enlazarlo.

## Notas

- RabbitMQ 4 trata las quorum queues con `x-delivery-limit` por defecto (20): un mensaje que falla de forma transitoria más veces
  que eso se descarta sin dead-letter. El backoff del consumidor (hasta 5 s) alarga el plazo, no lo elimina.
- Los usuarios tienen `configure: ^$`: los clientes no declaran colas ni exchanges (`amqpx` no declara nada).
