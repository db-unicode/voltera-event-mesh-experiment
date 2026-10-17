#!/bin/sh
# S2: lag de Kafka en Prometheus (kafka-exporter). Imprime PASS o FAIL.
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

NS=s02-kafka-lag
TOPIC=r2.energy-matched
GROUP=s02
COUNT=5000
LAG_Q="sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\",topic=\"$TOPIC\"})"
init_results s02
prefix="s02-$stamp"

# lag_between <min> <max>: el lag actual está en el rango (imprime el valor)
lag_between() {
  v=$(prom_scalar "$LAG_Q")
  echo "lag=${v:-sin datos}"
  [ -n "$v" ] && awk -v v="$v" -v lo="$1" -v hi="$2" 'BEGIN { exit !(v >= lo && v <= hi) }'
}

# wait_lag <min> <max> <segundos>: sondea cada 2 s; imprime en qué segundo se cumplió
wait_lag() {
  t0=$(date +%s)
  while :; do
    if lag_between "$1" "$2" >/dev/null 2>&1; then
      echo "alcanzado en $(($(date +%s) - t0)) s: $(lag_between "$1" "$2")"
      return 0
    fi
    [ $(($(date +%s) - t0)) -lt "$3" ] || {
      echo "no alcanzado en $3 s: $(lag_between "$1" "$2" 2>&1 || true)"
      return 1
    }
    sleep 2
  done
}

lag_rises() { wait_lag $((COUNT * 99 / 100)) $((COUNT * 101 / 100)) 30; }

# tras consumir, el exporter y Prometheus tardan hasta ~2 intervalos de scrape en reflejarlo
lag_drops() { wait_lag 0 0 60; }

produce_ok() {
  probe_job "$NS" s02-produce produce --topic "$TOPIC" --count "$COUNT" --key-prefix "$prefix" \
    && grep -q "produced=$COUNT " "$out/s02-produce.log"
}

consume_ok() {
  probe_job "$NS" s02-consume consume --topic "$TOPIC" --group "$GROUP" --expect "$COUNT" --timeout 120s --key-prefix "$prefix" \
    && grep -q "count=$COUNT distinct=$COUNT " "$out/s02-consume.log"
}

seek_ok() {
  probe_job "$NS" s02-seek seek --topic "$TOPIC" --group "$GROUP" --to end
}

# nombres exactos de las series, leídos del endpoint del exporter con curl desde un pod del mesh
exporter_series() {
  k -n "$NS" run curl --image=curlimages/curl:8.23.0 --restart=Never --command -- sleep 3600 >/dev/null 2>&1 || true
  k -n "$NS" wait --for=condition=Ready pod/curl --timeout=120s
  k -n "$NS" exec curl -c curl -- curl -fsS --max-time 10 http://kafka-exporter.voltera-infra:9308/metrics >"$out/exporter-metrics.txt"
  grep -E "^kafka_(consumergroup|topic_partition_current_offset)" "$out/exporter-metrics.txt" | grep -E "consumergroup=\"$GROUP\"|topic_partition_current_offset" >"$out/exporter-series.txt"
  for s in kafka_consumergroup_lag kafka_consumergroup_lag_sum kafka_consumergroup_current_offset kafka_topic_partition_current_offset; do
    grep -q "^$s{" "$out/exporter-series.txt" || {
      echo "falta $s"
      return 1
    }
  done
}

prom_has_series() {
  for s in kafka_consumergroup_lag kafka_consumergroup_lag_sum kafka_consumergroup_current_offset kafka_topic_partition_current_offset; do
    v=$(prom_scalar "count($s)")
    echo "$s: ${v:-0} series"
    [ -n "$v" ] || return 1
  done
}

log "S2: preparación"
check "StatefulSet kafka y kafka-exporter listos" kafka_ready
check "imagen kafka-probe construida y cargada en kind" build_probe_image
ensure_ns "$NS" >/dev/null
check "grupo $GROUP confirmado al final del log (lag de partida 0)" seek_ok
check "Prometheus ve lag 0 del grupo $GROUP" retry 90 lag_between 0 0

log "S2: producir $COUNT eventos sin consumidor"
check "produce $COUNT eventos" produce_ok
check "lag del grupo $GROUP llega a ~$COUNT en <= 30 s" lag_rises
check "series del exporter visibles con curl (lag, lag_sum, current_offset)" exporter_series
check "las mismas series existen en Prometheus" prom_has_series

log "S2: consumir"
check "consumidor del grupo $GROUP lee $COUNT/$COUNT con commit manual" consume_ok
check "lag del grupo $GROUP vuelve a 0" lag_drops

finish "$NS"
