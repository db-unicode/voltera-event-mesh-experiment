#!/bin/sh
# S5: KEDA + RabbitMQ, 0 -> N -> 0 y capacidad por pod (C_pod). Imprime PASS o FAIL.
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

NS=s05-keda-rabbit
DEPLOY=declaration-gateway-probe
QUEUE=meter-declarations
VHOST=r1
PF_MGMT=19202          # port-forward al management API (15672) de rabbitmq-0
PF_POD=19203           # port-forward al /metrics del pod de prueba
KEDA_VALUE=${KEDA_VALUE:-100}
FILL=4000              # mensajes para medir C_pod con la cola llena
MEASURE_S=30           # ventana de medición de C_pod
SCALE_IN_LIMIT_S=300   # tiempo máximo hasta volver a 0 tras el pico

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s05/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
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

PF_MGMT_PID=
PF_POD_PID=
SAMPLER_PID=
cleanup() {
  for p in "$SAMPLER_PID" "$PF_POD_PID" "$PF_MGMT_PID"; do
    [ -z "$p" ] || kill "$p" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

KEDA_PW=$(k -n voltera-infra get secret rabbitmq-users -o jsonpath='{.data.keda}' | base64 -d)

# depth: mensajes (ready + unacked) que ve KEDA en el management API, con el usuario keda
depth() {
  curl -fsS -m 5 -u "keda:$KEDA_PW" "http://127.0.0.1:$PF_MGMT/api/queues/$VHOST/$QUEUE" | jq -r '.messages'
}

replicas() { k -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.status.replicas}'; }

rmqctl() { k -n voltera-infra exec rabbitmq-0 -c rabbitmq -- rabbitmqctl -q "$@"; }

purge_queue() { rmqctl purge_queue -p "$VHOST" "$QUEUE"; }

depth_is_zero() { [ "$(depth)" = 0 ]; }

# empty_start: deja la cola en 0 por si quedó algo de una corrida anterior
empty_start() {
  purge_queue
  retry 20 depth_is_zero
}

pod_counter() { curl -fsS -m 5 "http://127.0.0.1:$PF_POD/metrics" | awk '$1 == "events_consumed_total" {print $2}'; }

prom() { k get --raw "/api/v1/namespaces/istio-system/services/prometheus:9090/proxy/api/v1/query?query=$1"; }

start_mgmt_forward() {
  kubectl --context "$KUBE_CONTEXT" -n voltera-infra port-forward pod/rabbitmq-0 "$PF_MGMT:15672" >"$out/port-forward-mgmt.log" 2>&1 &
  PF_MGMT_PID=$!
  retry 20 depth
}

rabbit_ready() { k -n voltera-infra wait --for=condition=Ready pod/rabbitmq-0 --timeout=120s; }

keda_ready() { k -n keda wait --for=condition=Available deploy/keda-operator --timeout=120s; }

setup_namespace() {
  k delete namespace "$NS" --ignore-not-found --wait=true
  k apply -f "$HERE/manifests.yaml"
  amqp_copy_secret "$NS"
  # URI del management API con el usuario keda (tag monitoring); el vhost va en la ruta
  k -n "$NS" create secret generic keda-rabbitmq-uri \
    --from-literal="host=http://keda:$KEDA_PW@rabbitmq.voltera-infra.svc:15672/$VHOST"
}

fill_queue() {
  amqp_run "$NS" meter-sim-r1 "$VHOST" publish --queue "$QUEUE" --count "$FILL" | jq -e --argjson c "$FILL" '.confirmed == $c'
}

# measure_cpod: con 1 réplica fija y la cola llena, msgs/s sostenidos por el contador del pod
measure_cpod() {
  k -n "$NS" scale "deploy/$DEPLOY" --replicas=1
  k -n "$NS" rollout status "deploy/$DEPLOY" --timeout=120s
  kubectl --context "$KUBE_CONTEXT" -n "$NS" port-forward "deploy/$DEPLOY" "$PF_POD:8080" >"$out/port-forward-pod.log" 2>&1 &
  PF_POD_PID=$!
  retry 30 pod_counter
  sleep 10   # calentamiento
  d0=$(depth)
  c0=$(pod_counter)
  t0=$(date +%s)
  sleep "$MEASURE_S"
  c1=$(pod_counter)
  t1=$(date +%s)
  d1=$(depth)
  kill "$PF_POD_PID" 2>/dev/null || true
  PF_POD_PID=
  echo "contador: $c0 -> $c1 en $((t1 - t0)) s; profundidad: $d0 -> $d1"
  [ "$d1" -gt 0 ] || { echo "la cola se vació durante la medición: C_pod no es sostenido"; return 1; }
  awk -v c0="$c0" -v c1="$c1" -v dt="$((t1 - t0))" 'BEGIN { printf "%.1f\n", (c1 - c0) / dt }' >"$out/c_pod.txt"
  echo "C_pod = $(cat "$out/c_pod.txt") msgs/s"
  awk -v c="$(cat "$out/c_pod.txt")" 'BEGIN { exit !(c > 5) }'
}

# prom_rate: contraste con rate(events_consumed_total[30s]) en Prometheus (informativo)
prom_rate() {
  prom "sum(rate(events_consumed_total%7Bnamespace%3D%22$NS%22%7D%5B30s%5D))" | tee "$out/prom-rate.json" \
    | jq -r '.data.result[0].value[1] // "sin datos"'
}

stop_and_drain() {
  k -n "$NS" scale "deploy/$DEPLOY" --replicas=0
  k -n "$NS" wait --for=delete pod -l "app=$DEPLOY" --timeout=90s
  purge_queue
  retry 20 depth_is_zero
}

apply_scaledobject() {
  sed "s/value: \"100\"/value: \"$KEDA_VALUE\"/" "$HERE/scaledobject.yaml" | k apply -f -
}

scaledobject_ready() {
  k -n "$NS" get scaledobject "$DEPLOY" -o json | jq -e '.status.conditions | map(select(.type == "Ready" and .status == "True")) | length == 1'
}

hpa_created() { k -n "$NS" get hpa "keda-hpa-$DEPLOY" -o json | jq -e '.spec.maxReplicas == 10 and .spec.behavior.scaleDown.stabilizationWindowSeconds == 30'; }

at_rest_zero() { [ "$(replicas)" = "" ] || [ "$(replicas)" = 0 ]; }

# sampler <archivo>: ts,replicas,depth cada 2 s
sampler() {
  echo "ts,replicas,depth" >"$1"
  while :; do
    r=$(replicas 2>/dev/null || true)
    d=$(depth 2>/dev/null || true)
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),${r:-0},${d:-}" >>"$1"
    sleep 2
  done
}

run_burst() {
  cpod=$(cat "$out/c_pod.txt")
  total=$(awk -v c="$cpod" 'BEGIN { printf "%d\n", 10 * c + 0.5 }')
  rate=$(awk -v c="$cpod" 'BEGIN { r = int(c + 0.5); if (r < 1) r = 1; print r }')
  echo "ráfaga: $total mensajes a $rate msgs/s (~$((total / rate)) s)"
  echo "$total $rate" >"$out/burst.txt"
  amqp_run "$NS" meter-sim-r1 "$VHOST" publish --queue "$QUEUE" --count "$total" --rate "$rate" \
    | jq -e --argjson c "$total" '.confirmed == $c'
}

# wait_scale_in: espera replicas=0 y profundidad=0 (última fila del CSV)
wait_scale_in() {
  t0=$(date +%s)
  while :; do
    last=$(tail -n 1 "$out/samples.csv")
    case $last in
      *,0,0) return 0 ;;
    esac
    [ $(($(date +%s) - t0)) -lt "$SCALE_IN_LIMIT_S" ] || { echo "sin volver a 0 en ${SCALE_IN_LIMIT_S}s; última muestra: $last"; return 1; }
    sleep 3
  done
}

# analyze_samples: 0 al inicio -> >= 2 en el pico -> 0 y cola 0 al final; imprime tiempos relativos al primer dato
analyze_samples() {
  awk -F, '
    function secs(ts) { return substr(ts, 12, 2) * 3600 + substr(ts, 15, 2) * 60 + substr(ts, 18, 2) }
    NR == 1 { next }
    NR == 2 { t0 = secs($1); first_r = $2 }
    {
      t = secs($1) - t0
      if ($2 > max) { max = $2; tmax = t }
      if ($2 >= 1 && up1 == "") up1 = t
      if ($2 >= 2 && up2 == "") up2 = t
      if (max >= 2 && $2 == 0 && down == "") down = t
      if ($2 > 0) down = ""
      last_r = $2; last_d = $3
    }
    END {
      printf "primera_muestra_replicas=%s pico=%s (t=%ss) primera_replica_t=%ss dos_replicas_t=%ss vuelta_a_cero_t=%ss final_replicas=%s final_profundidad=%s\n", first_r, max, tmax, up1, up2, down, last_r, last_d
      exit !(first_r == 0 && max >= 2 && last_r == 0 && last_d == 0)
    }' "$out/samples.csv"
}

peak_depth_seen() { awk -F, 'NR > 1 && $3 + 0 > m { m = $3 + 0 } END { print "profundidad máxima muestreada: " m; exit !(m > 0) }' "$out/samples.csv"; }

log "S5: requisitos"
check "RabbitMQ rabbitmq-0 Ready" rabbit_ready
check "KEDA operator Available" keda_ready
check "management API accesible (usuario keda) por port-forward" start_mgmt_forward
check "namespace $NS, Deployment, TriggerAuthentication y Secrets" setup_namespace

log "S5: C_pod con 1 réplica fija y la cola llena"
check "cola vacía antes de empezar (purga si hace falta)" empty_start
check "publicar $FILL mensajes (cola llena)" fill_queue
check "C_pod medido (ver c_pod.txt)" measure_cpod
check "contraste: rate(events_consumed_total[30s]) en Prometheus (informativo)" prom_rate
check "réplica fija a 0 y cola vaciada" stop_and_drain

log "S5: ScaledObject (value=$KEDA_VALUE)"
check "ScaledObject aplicado" apply_scaledobject
check "ScaledObject Ready" retry 60 scaledobject_ready
check "HPA keda-hpa-$DEPLOY con max 10 y stabilization 30 s" retry 30 hpa_created
check "en reposo: 0 réplicas" retry 60 at_rest_zero

log "S5: ráfaga y muestreo (ts,replicas,depth cada 2 s)"
sampler "$out/samples.csv" &
SAMPLER_PID=$!
sleep 4
check "ráfaga publicada (10 x C_pod en ~10 s)" run_burst
check "la cola acumuló mensajes durante la ráfaga" retry 10 peak_depth_seen
check "vuelve a 0 réplicas con cola 0 (<= ${SCALE_IN_LIMIT_S}s)" wait_scale_in
sleep 4
kill "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=
check "serie: 0 -> >= 2 -> 0 y profundidad final 0" analyze_samples

k -n "$NS" get scaledobject,hpa,deploy,pods -o wide >"$out/k8s-state.txt" 2>&1 || true
k -n "$NS" get events --sort-by=.lastTimestamp >"$out/events.txt" 2>&1 || true
k -n "$NS" get scaledobject "$DEPLOY" -o yaml >"$out/scaledobject.yaml" 2>&1 || true
k -n "$NS" describe hpa >"$out/hpa-describe.txt" 2>&1 || true
{ echo "C_pod=$(cat "$out/c_pod.txt" 2>/dev/null || echo n/a) msgs/s"; echo "KEDA_VALUE=$KEDA_VALUE"; } | tee "$out/summary.txt"

if [ "$failures" -eq 0 ]; then
  k delete namespace "$NS" --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
