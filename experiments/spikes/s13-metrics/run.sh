#!/bin/sh
# S13: las métricas Go de un servicio llegan a Prometheus (Istio, merge de métricas en :15020).
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"

need kubectl
need kind
need docker
need git
need jq

NS=s13-metrics

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s13/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
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

# retry <segundos> <comando...>: reintenta cada 5 s hasta que el comando pase
retry() {
  limit=$1
  shift
  elapsed=0
  until "$@"; do
    [ "$elapsed" -lt "$limit" ] || return 1
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

# prom <promql>: consulta instantánea; imprime el JSON
prom() {
  q=$(jq -rn --arg q "$1" '$q | @uri')
  k get --raw "/api/v1/namespaces/istio-system/services/prometheus:9090/proxy/api/v1/query?query=$q"
}

# prom_value <promql>: valor numérico de la primera serie (vacío si no hay)
prom_value() { prom "$1" | jq -r '.data.result[0].value[1] // empty'; }

has_series() {
  v=$(prom_value "$1")
  echo "$1 => ${v:-sin series}"
  [ -n "$v" ]
}

probe_pod() { k -n "$NS" get pod -l app=metrics-probe -o jsonpath='{.items[0].metadata.name}'; }

annotation_rewritten() {
  k -n "$NS" get pod -l app=metrics-probe -o json \
    | tee "$out/pod.json" \
    | jq -e '.items[0].metadata.annotations | ."prometheus.io/port" == "15020" and ."prometheus.io/path" == "/stats/prometheus"'
}

target_is_15020() {
  pod=$(probe_pod)
  k get --raw "/api/v1/namespaces/istio-system/services/prometheus:9090/proxy/api/v1/targets?state=active" >"$out/targets.json"
  jq -e --arg pod "$pod" '
    [.data.activeTargets[] | select(.labels.pod == $pod and .labels.namespace == "'"$NS"'" and .health == "up")]
    | length > 0 and all(.[]; .scrapeUrl | contains(":15020/stats/prometheus"))' "$out/targets.json"
}

counter_grows() {
  a=$(prom_value 'sum(events_consumed_total{service="metrics-probe"})')
  sleep 20
  b=$(prom_value 'sum(events_consumed_total{service="metrics-probe"})')
  echo "antes=$a despues=$b"
  [ -n "$a" ] && [ -n "$b" ] && awk -v a="$a" -v b="$b" 'BEGIN { exit !(b > a) }'
}

log "S13: despliegue"
check "imagen voltera/metrics-probe:dev construida y cargada en kind" sh -c "cd '$ROOT' && mise run images:build metrics-probe"
k apply -f "$HERE/manifests.yaml" >/dev/null
check "metrics-probe Ready (app + sidecar)" k -n "$NS" rollout status deployment/metrics-probe --timeout=180s
check "curl Ready" k -n "$NS" rollout status deployment/curl --timeout=180s
check "Istio reescribió prometheus.io/port a 15020 (merge de métricas)" annotation_rewritten

log "S13: Prometheus (el scrape es cada 15 s; hasta 2 min)"
check "Prometheus raspa el pod por :15020/stats/prometheus (target up)" retry 120 target_is_15020
check "events_consumed_total{service=metrics-probe} visible en Prometheus" retry 120 has_series 'events_consumed_total{service="metrics-probe",result="ok"}'
check "events_consumed_total con result=duplicate y result=stale" has_series 'events_consumed_total{service="metrics-probe",result="stale"}'
check "event_e2e_lag_seconds (histograma) visible" retry 60 has_series 'event_e2e_lag_seconds_count{service="metrics-probe",stage="occurred"}'
check "métricas de runtime de Go (go_goroutines) visibles" has_series 'go_goroutines{service="metrics-probe"}'
check "istio_requests_total hacia metrics-probe visible junto a las de la app" retry 90 has_series 'istio_requests_total{destination_service_name="metrics-probe",destination_workload_namespace="s13-metrics"}'
check "events_consumed_total crece en 20 s" counter_grows

prom 'events_consumed_total{service="metrics-probe"}' >"$out/events_consumed_total.json" || true
prom 'istio_requests_total{destination_service_name="metrics-probe"}' >"$out/istio_requests_total.json" || true
k -n "$NS" top pods >"$out/top-pods.txt" 2>&1 || true

if [ "$failures" -eq 0 ]; then
  k delete namespace "$NS" --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
