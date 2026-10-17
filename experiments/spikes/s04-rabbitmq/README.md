# S4. RabbitMQ con colas durables y profundidad en Prometheus

RabbitMQ 4.3.6 (`rabbitmq:4.3.6-management`, multi-arch con `linux/arm64/v8`) en `voltera-infra`, con sidecar Istio, más el cliente de prueba `amqp-probe`.

## Ejecutar

```sh
mise run cluster:up && mise run platform:up && mise run base:up   # una vez
mise run brokers:rabbitmq && mise run images:build amqp-probe
mise run spike:s04
```

`spike:s04` imprime `PASS|FAIL <check>` y termina en `PASS` (exit 0) o `FAIL` (exit 1). Evidencia en `experiments/results/s04/<UTC>-<sha>/`. Al pasar borra el namespace `s04-rabbitmq`.

## Qué se despliega (`deploy/brokers/rabbitmq/`)

| Archivo | Contenido |
|---|---|
| `statefulset.yaml` | 1 réplica, PVC `data` de 1Gi, init container que renderiza las definiciones, anotaciones de scrape |
| `service.yaml` | `rabbitmq` (`tcp-amqp` 5672, `http-management` 15672, `http-prometheus` 15692) y `rabbitmq-headless` |
| `kustomization.yaml` | ConfigMap `rabbitmq-config` y Secret `rabbitmq-users` (`secretGenerator`, una clave por usuario) |
| `definitions.json.tmpl` | vhosts, usuarios, permisos, colas, exchanges y bindings (contraseñas como `@@usuario@@`) |
| `rabbitmq.conf` | `load_definitions`, `vm_memory_high_watermark.absolute = 384MiB` |
| `security.yaml`, `network-policy.yaml` | acceso de KEDA al puerto 15672 (ver S5) |

Las contraseñas viven solo en el Secret. Un init container (misma imagen) sustituye `@@usuario@@` por el contenido de `/secrets/<usuario>` y deja `definitions.json` en un `emptyDir` en memoria; RabbitMQ lo carga al arrancar (`load_definitions`). `disableNameSuffixHash` solo en el Secret: los clientes de otros namespaces lo copian por nombre.

### Usuarios y permisos

Patrón `<servicio>-<región>`, un usuario por servicio y vhost. Todos con `configure = ^$`: las colas las crean las definiciones, ningún servicio declara nada.

| Usuario | write | read |
|---|---|---|
| `meter-sim-<r>` | `^meter-declarations\.x$` | `^$` |
| `declaration-gateway-<r>` | `^energy-positions\.x$` | `^meter-declarations$` |
| `price-feed-<r>` | `^kwh-prices\.x$` | `^$` |
| `market-projector-<r>` | `^$` | `^(energy-positions\|kwh-prices)$` |
| `keda` (tag `monitoring`, vhosts r1 y r2) | `^$` | `^$` |
| `admin` (tag `administrator`) | `.*` | `.*` |

### Cómo se restringe quién publica en qué cola

`basic.publish` va a un exchange, no a una cola. Al publicar en el exchange por defecto (`""`), RabbitMQ evalúa `write` sobre `amq.default`, y ese exchange enruta a cualquier cola por nombre. Dar `write` sobre `amq.default` equivale a poder escribir en todas las colas. La solución aplicada:

- Un exchange `direct` durable por cola, `<cola>.x`, con un binding a su cola y routing key = nombre de la cola.
- Los publicadores reciben `write` solo sobre el regex de su exchange y no sobre `amq.default`.
- `amqp-probe publish` usa `<cola>.x` si no se pasa `--exchange`. Publica con `mandatory=true`: si no hay binding, el broker devuelve el mensaje y el comando falla.
- Consumir exige `read` sobre la cola. No hace falta `configure` (ni declaración pasiva).

Comprobado en `spike:s04`: `meter-sim-r1` publicando en `energy-positions.x` recibe `403 ACCESS_REFUSED - write access to exchange 'energy-positions.x'`; por el exchange por defecto, `403 ... 'amq.default'`; en el vhost `r2`, `403 no access to this vhost`. La matriz Zero Trust completa queda para otro spike; la estructura ya está.

## Nombres exactos de las series de profundidad

Verificados con `curl` contra el plugin (puerto 15692, `spike:s04` guarda la salida completa):

| Endpoint | Series (etiquetas `vhost`, `queue`) |
|---|---|
| `/metrics/per-object` | `rabbitmq_queue_messages_ready`, `rabbitmq_queue_messages_unacked`, `rabbitmq_queue_messages` (ready + unacked) |
| `/metrics/detailed?family=queue_coarse_metrics` | `rabbitmq_detailed_queue_messages_ready`, `rabbitmq_detailed_queue_messages_unacked`, `rabbitmq_detailed_queue_messages` |

`/metrics` a secas (`prometheus.return_per_object_metrics = false`) devuelve solo agregados, sin etiqueta `queue`.

Prometheus scrapea `/metrics/per-object` por las anotaciones del pod (`prometheus.io/scrape`, `prometheus.io/port: "15692"`, `prometheus.io/path: /metrics/per-object`). Como Prometheus está fuera de la malla y el mTLS es STRICT, no llega al 15692: el sidecar fusiona las métricas del contenedor en `:15020/stats/prometheus` (metrics merging) y la serie aparece con `job="kubernetes-pods"`, `instance="<ip>:15020"`. Consulta usada: `rabbitmq_queue_messages_ready{vhost="r1",queue="meter-declarations"}`.

## Qué comprueba

1. StatefulSet Ready con app + `istio-proxy`, PVC de 1Gi enlazado, puertos del Service.
2. Definiciones: 3 quorum queues durables por vhost, 10 usuarios (solo `keda` con `monitoring`), sin `guest`, un exchange `<cola>.x` por cola, permisos exactos de 5 usuarios.
3. Series de profundidad (nombres arriba) y scrape de Prometheus.
4. `meter-sim-r1` publica 2000 persistentes con confirms; Prometheus muestra 2000 en <= 30 s.
5. Tres operaciones denegadas (arriba) y `energy-positions` sigue en 0.
6. `declaration-gateway-r1` consume 2000 con ack manual (prefetch 50); Prometheus vuelve a 0 en <= 30 s.
7. Durabilidad: 50 mensajes persistentes sobreviven a `kubectl delete pod rabbitmq-0`.
8. `kubectl top` de `rabbitmq-0`.

Los clientes corren como Jobs con sidecar en `s04-rabbitmq` (`experiments/lib/amqp.sh`), con la contraseña del Secret copiado, para pasar por el mismo mTLS que usará el resto de servicios.

## Resultados medidos

- Prometheus ve 2000 a los 7-10 s del fin de la publicación y 0 a los 15 s del fin del consumo (scrape cada 15 s + estadísticas de cola cada 5 s). Medidas en `depth-latency.txt`.
- RAM en reposo tras las pruebas: `rabbitmq` 143 MiB (límite 640 MiB), `istio-proxy` 33 MiB. El estimado del informe 02 era 0,45 GiB.

## Hallazgos

- **`guest` sigue vivo con la imagen oficial.** `10-defaults.conf` pone `loopback_users.guest = false`, así que `guest/guest` entraría desde cualquier pod. Se fija `RABBITMQ_DEFAULT_USER=admin` (con contraseña del Secret) y `guest` no se crea.
- **Las definiciones deben declarar el vhost `/`** si algún permiso lo menciona; si no, RabbitMQ aborta el arranque con `Please create virtual host "/" prior to importing definitions`.
- **`/metrics/per-object` pesa.** Con 6 colas y pocas conexiones devuelve ~2600 líneas por scrape; sube con cada cola, canal y conexión.
- **`prometheus.io/path` lleva solo la ruta.** No se probó un query string en la anotación con el merging del sidecar; `per-object` no lo necesita, así que se usa ese endpoint y `detailed?family=...` queda para consultas manuales.
- **`kubeconform` lee todo `.json` de `deploy/`**: la plantilla se llama `.json.tmpl`.
