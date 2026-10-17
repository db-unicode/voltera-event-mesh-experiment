#!/bin/sh
# S3: KEDA con trigger Kafka escala energy-transfer-probe 0 -> N -> 0. Imprime PASS o FAIL.
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"
# shellcheck source=experiments/lib/kafka-spikes.sh
. "$HERE/../../lib/kafka-spikes.sh"

need kubectl
need docker
need kind
need jq
need git
require_cluster

NS=s03-keda-kafka
TOPIC=r1.energy-matched
GROUP=energy-transfer-probe
COUNT=3000
RATE=300   # eventos/s: 3000 en ~10 s
init_results s03
prefix="s03-$stamp"
csv="$out/s03-samples.csv"
sampler_pid=""

cleanup() { [ -z "$sampler_pid" ] || kill "$sampler_pid" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# replicas del Deployment sin pasar por KEDA: pods existentes (listos o no)
replicas() { k -n "$NS" get deploy energy-transfer-probe -o jsonpath='{.status.replicas}'; }

# lag total del grupo, leído directo del exporter (se calcula en cada petición, sin esperar el scrape)
exporter_lag() {
  k -n "$NS" exec curl -c curl -- curl -fsS --max-time 3 http://kafka-exporter.voltera-infra:9308/metrics \
    | awk -v g="$GROUP" -v t="$TOPIC" '$0 ~ "^kafka_consumergroup_lag_sum\\{consumergroup=\"" g "\",topic=\"" t "\"\\}" { s += $2 } END { print s + 0 }'
}

# sample_loop: una fila ts,replicas,lag cada ~2 s
sample_loop() {
  echo "ts,replicas,lag" >"$csv"
  while :; do
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    r=$(replicas || true)
    l=$(exporter_lag || true)
    echo "$ts,${r:-0},${l:-NA}" >>"$csv"
    sleep $((2 - $(date +%s) % 2))
  done
}

no_pods() { [ -z "$(k -n "$NS" get pods -l app=energy-transfer-probe -o name)" ]; }

prepare() {
  ensure_ns "$NS"
  k apply -f "$HERE/deployment.yaml"
  # parte de cero: sin ScaledObject, sin réplicas y con el grupo al final del log
  k -n "$NS" delete scaledobject energy-transfer-probe --ignore-not-found --wait=true
  k -n "$NS" scale deploy/energy-transfer-probe --replicas=0
  retry 120 no_pods
  k -n "$NS" get pod curl >/dev/null 2>&1 \
    || k -n "$NS" run curl --image=curlimages/curl:8.23.0 --restart=Never --command -- sleep 3600
  k -n "$NS" wait --for=condition=Ready pod/curl --timeout=120s
}

seek_ok() { probe_job "$NS" s03-seek seek --topic "$TOPIC" --group "$GROUP" --to end; }

scaledobject_ready() {
  k apply -f "$HERE/scaledobject.yaml"
  retry 60 so_ready
}

so_ready() {
  k -n "$NS" get scaledobject energy-transfer-probe -o json >"$out/scaledobject.json"
  jq -e '.status.conditions[] | select(.type == "Ready") | .status == "True"' "$out/scaledobject.json"
}

idle_at_zero() {
  sleep 12
  r=$(replicas)
  echo "replicas=${r:-0}"
  [ "${r:-0}" = 0 ]
}

produce_ok() {
  probe_job "$NS" s03-produce produce --topic "$TOPIC" --count "$COUNT" --key-prefix "$prefix" --rate "$RATE" \
    && grep -q "produced=$COUNT " "$out/s03-produce.log"
}

produce_took_about_10s() {
  secs=$(sed -n 's/.*elapsed=\([0-9]*\)\(\.[0-9]*\)\{0,1\}s.*/\1/p' "$out/s03-produce.log")
  echo "elapsed=${secs:-?}s"
  [ -n "$secs" ] && [ "$secs" -ge 8 ] && [ "$secs" -le 15 ]
}

# true cuando el CSV muestra 0 -> >0 -> 0 réplicas con lag final 0
scaled_up_and_down() {
  awk -F, 'NR > 1 && $2 > 0 { up = 1 } NR > 1 { r = $2; l = $3 } END { exit !(up && r == 0 && l == 0) }' "$csv"
}

wait_scaled_down() {
  t0=$(date +%s)
  until scaled_up_and_down; do
    [ $(($(date +%s) - t0)) -lt 420 ] || return 1
    sleep 3
  done
  echo "ciclo completo en $(($(date +%s) - t0)) s tras producir"
}

# resumen del CSV: primera fila, máximo, última fila
curve_ok() {
  awk -F, '
    NR == 2 { first = $2 }
    NR > 1 { if ($2 + 0 > max) { max = $2 + 0; peak_row = NR }; last_r = $2; last_l = $3; if ($3 + 0 > maxlag) maxlag = $3 + 0 }
    END {
      printf "primera=%s max=%d ultima=%s lag_max=%d lag_final=%s filas=%d\n", first, max, last_r, maxlag, last_l, NR - 1
      exit !(first == 0 && max >= 2 && max <= 6 && last_r == 0 && last_l == 0 && maxlag >= COUNT * 0.5)
    }' COUNT="$COUNT" "$csv"
}

prom_final_lag_zero() {
  v=$(prom_scalar "sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\",topic=\"$TOPIC\"})")
  echo "lag en Prometheus: ${v:-sin datos}"
  [ "${v:-}" = 0 ]
}

log "S3: preparación"
check "StatefulSet kafka y kafka-exporter listos" kafka_ready
check "imagen kafka-probe construida y cargada en kind" build_probe_image
check "Deployment energy-transfer-probe en 0 réplicas y pod curl listo" prepare
check "grupo $GROUP confirmado al final de $TOPIC (sin backlog previo)" seek_ok
check "ScaledObject (trigger kafka) Ready: KEDA llega al broker" scaledobject_ready
check "sin lag, KEDA mantiene 0 réplicas" idle_at_zero

log "S3: producir $COUNT eventos en ~10 s y muestrear réplicas/lag cada 2 s"
sample_loop &
sampler_pid=$!
sleep 6
check "produce $COUNT eventos a $RATE/s" produce_ok
check "la producción tardó ~10 s (8-15 s)" produce_took_about_10s
check "réplicas 0 -> N -> 0 con lag final 0 (hasta 7 min)" wait_scaled_down
cleanup
sampler_pid=""
check "curva: arranca en 0, pico entre 2 y 6, termina en 0, lag final 0 (pico de lag >= 50 % de lo producido)" curve_ok
check "Prometheus confirma lag 0 del grupo $GROUP" retry 60 prom_final_lag_zero

k -n "$NS" get scaledobject,hpa -o yaml >"$out/scaledobject-hpa.yaml" 2>&1 || true
k -n "$NS" describe scaledobject energy-transfer-probe >"$out/scaledobject-describe.txt" 2>&1 || true
k -n keda logs deploy/keda-operator -c keda-operator --since=15m >"$out/keda-operator.log" 2>&1 || true
prom_query "sum(max_over_time(events_consumed_total{group=\"$GROUP\"}[15m]))" >"$out/prom-events-consumed.json" 2>&1 || true
k top pods -A >"$out/top-pods.txt" 2>&1 || true
log "muestras en ${csv#"$ROOT"/}"
finish "$NS"
