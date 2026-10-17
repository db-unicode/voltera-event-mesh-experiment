#!/bin/sh
# S4: RabbitMQ con colas durables y métrica de profundidad en Prometheus. Imprime PASS o FAIL.
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"
# shellcheck source=experiments/lib/amqp.sh
. "$HERE/../../lib/amqp.sh"

need kubectl
need jq
need git
need curl
require_cluster

NS=s04-rabbitmq
PF_PORT=19201
COUNT=2000
DEPTH_DEADLINE_S=30   # el criterio del spike: profundidad visible en Prometheus en <= 30 s

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s04/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
mkdir -p "$out/checks"
log "evidencia en ${out#"$ROOT"/}"

n=0
failures=0
: >"$out/result.txt"

# check <descripción> <comando...>: registra PASS/FAIL sin abortar el spike
check() {
  desc=$1
  shift
  n=$((n + 1))
  if "$@" >"$out/checks/$n.log" 2>&1; then
    line="PASS  $desc"
  else
    line="FAIL  $desc (ver checks/$n.log)"
    failures=$((failures + 1))
  fi
  echo "$line" | tee -a "$out/result.txt"
}

# retry <segundos> <comando...>: reintenta cada 2 s hasta que el comando pase
retry() {
  limit=$1
  shift
  elapsed=0
  until "$@"; do
    [ "$elapsed" -lt "$limit" ] || return 1
    sleep 2
    elapsed=$((elapsed + 2))
  done
}

PF_PID=
cleanup() { [ -z "$PF_PID" ] || kill "$PF_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

prom() {
  k get --raw "/api/v1/namespaces/istio-system/services/prometheus:9090/proxy/api/v1/query?query=$1"
}

# prom_depth <vhost> <cola>: valor actual de rabbitmq_queue_messages_ready (vacío si no hay serie)
prom_depth() {
  prom "rabbitmq_queue_messages_ready%7Bvhost%3D%22$1%22%2Cqueue%3D%22$2%22%7D" | jq -r '.data.result[0].value[1] // empty'
}

# depth_is <vhost> <cola> <valor>
depth_is() { [ "$(prom_depth "$1" "$2")" = "$3" ]; }

rmqctl() { k -n voltera-infra exec rabbitmq-0 -c rabbitmq -- rabbitmqctl -q "$@"; }

rabbit_ready() { k -n voltera-infra wait --for=condition=Ready pod/rabbitmq-0 --timeout=120s; }

one_replica_two_containers() {
  k -n voltera-infra get pod rabbitmq-0 -o json | jq -e '
    [(.status.containerStatuses // []), (.status.initContainerStatuses // [])] | add
    | map(select(.name == "istio-proxy" or .name == "rabbitmq")) | length == 2 and all(.[]; .ready)'
}

pvc_bound() { k -n voltera-infra get pvc data-rabbitmq-0 -o json | jq -e '.status.phase == "Bound" and .spec.resources.requests.storage == "1Gi"'; }

service_ports() {
  k -n voltera-infra get svc rabbitmq -o json | jq -e '
    .spec.ports | map({(.name): .port}) | add
    | .["tcp-amqp"] == 5672 and .["http-management"] == 15672 and .["http-prometheus"] == 15692'
}

# queues_ok <vhost>: las tres colas existen, son quorum y durables
queues_ok() {
  rmqctl list_queues -p "$1" name type durable --formatter json | tee "$out/queues-$1.json" \
    | jq -e 'map(select(.type == "quorum" and .durable == true) | .name) | sort == ["energy-positions", "kwh-prices", "meter-declarations"]'
}

# users_ok: los 10 usuarios, con el tag monitoring solo en keda
users_ok() {
  rmqctl list_users --formatter json | tee "$out/users.json" | jq -e '
    (map(.user) | sort) == ["admin", "declaration-gateway-r1", "declaration-gateway-r2", "keda", "market-projector-r1",
      "market-projector-r2", "meter-sim-r1", "meter-sim-r2", "price-feed-r1", "price-feed-r2"]
    and (map(select(.tags | index("monitoring")) | .user) == ["keda"])'
}

# perms_ok <vhost> <usuario> <configure> <write> <read>: coincidencia exacta con la matriz de la spec
perms_ok() {
  rmqctl list_permissions -p "$1" --formatter json | jq -e --arg u "$2" --arg c "$3" --arg w "$4" --arg r "$5" '
    map(select(.user == $u)) | length == 1 and .[0].configure == $c and .[0].write == $w and .[0].read == $r'
}

guest_absent() { ! rmqctl list_users --formatter json | jq -e 'map(.user) | index("guest")'; }

exchanges_ok() {
  rmqctl list_bindings -p r1 --formatter json | jq -e '
    [.[] | select(.source_name | endswith(".x")) | "\(.source_name)>\(.destination_name)=\(.routing_key)"] | sort
    == ["energy-positions.x>energy-positions=energy-positions", "kwh-prices.x>kwh-prices=kwh-prices",
        "meter-declarations.x>meter-declarations=meter-declarations"]'
}

start_port_forward() {
  kubectl --context "$KUBE_CONTEXT" -n voltera-infra port-forward pod/rabbitmq-0 "$PF_PORT:15692" >"$out/port-forward.log" 2>&1 &
  PF_PID=$!
  retry 20 curl -fsS "http://127.0.0.1:$PF_PORT/metrics" -o /dev/null
}

# nombres exactos de las series de profundidad, directo del plugin (endpoint per-object y detailed)
metric_names() {
  curl -fsS "http://127.0.0.1:$PF_PORT/metrics/per-object" >"$out/metrics-per-object.txt"
  curl -fsS "http://127.0.0.1:$PF_PORT/metrics/detailed?family=queue_coarse_metrics&vhost=r1" >"$out/metrics-detailed-queue-coarse.txt"
  grep -E '^rabbitmq_queue_messages_ready\{vhost="r1",queue="meter-declarations"\}' "$out/metrics-per-object.txt"
  grep -E '^rabbitmq_queue_messages_unacked\{vhost="r1",queue="meter-declarations"\}' "$out/metrics-per-object.txt"
  grep -E '^rabbitmq_detailed_queue_messages_ready\{vhost="r1",queue="meter-declarations"\}' "$out/metrics-detailed-queue-coarse.txt"
  grep -E '^rabbitmq_detailed_queue_messages_unacked\{vhost="r1",queue="meter-declarations"\}' "$out/metrics-detailed-queue-coarse.txt"
}

prom_has_series_fn() { [ -n "$(prom_depth r1 meter-declarations)" ]; }

publish_2000() {
  amqp_run "$NS" meter-sim-r1 r1 publish --queue meter-declarations --count "$COUNT" | tee "$out/publish.json" \
    | jq -e --argjson c "$COUNT" '.published == $c and .confirmed == $c and .nacked == 0 and .returned == 0'
}

# prom_shows_2000: mide cuántos segundos tarda Prometheus en ver 2000 y exige <= DEPTH_DEADLINE_S
prom_shows_2000() {
  t0=$(date +%s)
  until depth_is r1 meter-declarations "$COUNT"; do
    [ $(($(date +%s) - t0)) -lt "$DEPTH_DEADLINE_S" ] || { echo "no llegó a $COUNT en ${DEPTH_DEADLINE_S}s (último: $(prom_depth r1 meter-declarations))"; return 1; }
    sleep 1
  done
  echo "Prometheus vio $COUNT tras $(($(date +%s) - t0)) s del fin de la publicación" | tee "$out/depth-latency.txt"
}

consume_all() {
  amqp_run "$NS" declaration-gateway-r1 r1 consume --queue meter-declarations --expect "$COUNT" --prefetch 50 | tee "$out/consume.json" \
    | jq -e --argjson c "$COUNT" '.consumed == $c'
}

prom_shows_0() {
  t0=$(date +%s)
  until depth_is r1 meter-declarations 0; do
    [ $(($(date +%s) - t0)) -lt "$DEPTH_DEADLINE_S" ] || { echo "no volvió a 0 en ${DEPTH_DEADLINE_S}s (último: $(prom_depth r1 meter-declarations))"; return 1; }
    sleep 1
  done
  echo "Prometheus vio 0 tras $(($(date +%s) - t0)) s del fin del consumo" | tee -a "$out/depth-latency.txt"
}

# denied <usuario> <vhost> <args de publish...>: el publish debe fallar con 403 (ACCESS_REFUSED, o "no access to this vhost" al conectar)
denied() {
  user=$1
  vhost=$2
  shift 2
  if amqp_run "$NS" "$user" "$vhost" "$@" >"$out/denied.tmp" 2>&1; then
    cat "$out/denied.tmp"
    echo "se esperaba un 403 y la operación funcionó"
    return 1
  fi
  cat "$out/denied.tmp"
  grep -q '(403)' "$out/denied.tmp"
}

# la cola sigue vacía tras los intentos denegados (nada se coló)
queue_empty() { depth_is "$1" "$2" 0; }

# durabilidad: 50 mensajes persistentes sobreviven al reinicio del pod
survives_restart() {
  amqp_run "$NS" meter-sim-r1 r1 publish --queue meter-declarations --count 50 >/dev/null
  cleanup
  PF_PID=
  k -n voltera-infra delete pod rabbitmq-0 --wait=true >/dev/null
  k -n voltera-infra wait --for=condition=Ready pod/rabbitmq-0 --timeout=180s
  amqp_run "$NS" declaration-gateway-r1 r1 consume --queue meter-declarations --expect 50 | jq -e '.consumed == 50'
}

log "S4: despliegue de RabbitMQ"
check "StatefulSet rabbitmq-0 Ready" rabbit_ready
check "rabbitmq-0 tiene app + istio-proxy listos (sidecar)" one_replica_two_containers
check "PVC data-rabbitmq-0 de 1Gi enlazado" pvc_bound
check "Service rabbitmq expone tcp-amqp 5672, http-management 15672, http-prometheus 15692" service_ports

log "S4: definiciones cargadas al arrancar"
check "vhost r1: 3 quorum queues durables" queues_ok r1
check "vhost r2: 3 quorum queues durables" queues_ok r2
check "10 usuarios; solo keda con tag monitoring" users_ok
check "no existe el usuario guest" guest_absent
check "exchange <cola>.x direct enlazado a su cola" exchanges_ok
check "permisos meter-sim-r1: escribe solo meter-declarations.x" perms_ok r1 meter-sim-r1 '^$' '^meter-declarations\.x$' '^$'
check "permisos declaration-gateway-r1: lee meter-declarations, escribe energy-positions.x" \
  perms_ok r1 declaration-gateway-r1 '^$' '^energy-positions\.x$' '^meter-declarations$'
check "permisos price-feed-r2: escribe solo kwh-prices.x" perms_ok r2 price-feed-r2 '^$' '^kwh-prices\.x$' '^$'
check "permisos market-projector-r1: lee energy-positions y kwh-prices" \
  perms_ok r1 market-projector-r1 '^$' '^$' '^(energy-positions|kwh-prices)$'
check "permisos keda r1: sin configure/write/read" perms_ok r1 keda '^$' '^$' '^$'

log "S4: métricas de profundidad"
check "port-forward al plugin prometheus (15692, puerto local $PF_PORT)" start_port_forward
check "series rabbitmq_queue_messages_ready/unacked y rabbitmq_detailed_queue_messages_ready/unacked" metric_names
check "Prometheus scrapea rabbitmq (serie rabbitmq_queue_messages_ready de r1/meter-declarations)" retry 60 prom_has_series_fn

log "S4: publicar $COUNT sin consumidor"
k create namespace "$NS" --dry-run=client -o yaml | k apply -f - >/dev/null
k label namespace "$NS" istio-injection=enabled --overwrite >/dev/null
amqp_copy_secret "$NS"
check "cola r1/meter-declarations vacía al inicio" retry 30 depth_is r1 meter-declarations 0
check "meter-sim-r1 publica $COUNT persistentes con confirms" publish_2000
check "Prometheus muestra $COUNT en <= ${DEPTH_DEADLINE_S} s" prom_shows_2000

log "S4: permisos negados (meter-sim-r1)"
check "meter-sim-r1 NO puede publicar en energy-positions (exchange energy-positions.x)" \
  denied meter-sim-r1 r1 publish --queue energy-positions --count 1
check "meter-sim-r1 NO puede publicar por el exchange por defecto (amq.default)" \
  denied meter-sim-r1 r1 publish --queue energy-positions --exchange "" --count 1
check "meter-sim-r1 NO puede entrar al vhost r2" denied meter-sim-r1 r2 publish --queue meter-declarations --count 1
check "energy-positions r1 sigue vacía" queue_empty r1 energy-positions

log "S4: consumir"
check "declaration-gateway-r1 consume $COUNT con ack manual (prefetch 50)" consume_all
check "Prometheus vuelve a 0 en <= ${DEPTH_DEADLINE_S} s" prom_shows_0

log "S4: durabilidad"
check "50 mensajes persistentes sobreviven al reinicio del pod" survives_restart

top_rabbit() { k top pods -n voltera-infra --containers >"$out/top-pods.txt" && grep -q '^rabbitmq-0 ' "$out/top-pods.txt"; }

log "S4: recursos"
check "kubectl top muestra rabbitmq-0" retry 120 top_rabbit
k -n voltera-infra get pods -o wide >"$out/pods.txt"

if [ "$failures" -eq 0 ]; then
  k delete namespace "$NS" --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
