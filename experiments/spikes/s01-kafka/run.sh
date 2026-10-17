#!/bin/sh
# S1: Kafka con sidecar Istio: produce, consume y replay con grupo nuevo. Imprime PASS o FAIL.
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

NS=s01-kafka
TOPIC=r2.energy-matched
COUNT=1000
init_results s01
prefix="s01-$stamp"
group_a="s01-a-$stamp"
group_b="s01-b-$stamp"

produce_ok() {
  probe_job "$NS" s01-produce produce --topic "$TOPIC" --count "$COUNT" --key-prefix "$prefix" \
    && grep -q "produced=$COUNT " "$out/s01-produce.log"
}

consume_ok() { # <job> <grupo>
  probe_job "$NS" "$1" consume --topic "$TOPIC" --group "$2" --expect "$COUNT" --timeout 120s --key-prefix "$prefix" \
    && grep -q "count=$COUNT distinct=$COUNT " "$out/$1.log"
}

# el Job termina solo aunque el sidecar siga vivo: pod en Succeeded, istio-proxy como init container nativo
job_terminated_alone() {
  phase=$(k -n "$NS" get pod -l job-name=s01-produce -o jsonpath='{.items[0].status.phase}')
  [ "$phase" = Succeeded ] || {
    echo "fase: $phase"
    return 1
  }
  k -n "$NS" get pod -l job-name=s01-produce -o jsonpath='{range .items[0].spec.initContainers[*]}{.name}={.restartPolicy}{"\n"}{end}' \
    | grep -qx 'istio-proxy=Always'
}

topic_config() {
  k -n voltera-infra logs job/kafka-topics -c create-topics >"$out/topics-describe.txt"
  for t in r1.energy-matched r2.energy-matched; do
    grep "^Topic: $t" "$out/topics-describe.txt" \
      | grep -q 'PartitionCount: 6.*ReplicationFactor: 1.*min.insync.replicas=1,retention.ms=604800000' || return 1
  done
}

# series istio_tcp_* hacia el workload kafka, reportadas por el destino (ve la identidad del origen)
tcp_mtls_series() {
  q="sum(istio_tcp_connections_opened_total{destination_workload=\"kafka\",destination_workload_namespace=\"voltera-infra\",reporter=\"destination\",connection_security_policy=\"mutual_tls\"})"
  v=$(prom_scalar "$q")
  echo "$q = ${v:-sin datos}"
  prom_query "istio_tcp_connections_opened_total{destination_workload=\"kafka\"}" >"$out/prom-istio-tcp.json"
  [ -n "$v" ] && awk -v v="$v" 'BEGIN { exit !(v > 0) }'
}

# la conexión del Job aparece con su identidad como origen (mTLS de verdad, no solo del lado del broker)
tcp_mtls_from_job() {
  q="sum(istio_tcp_connections_opened_total{destination_workload=\"kafka\",reporter=\"destination\",connection_security_policy=\"mutual_tls\",source_workload_namespace=\"$NS\"})"
  v=$(prom_scalar "$q")
  echo "$q = ${v:-sin datos}"
  [ -n "$v" ] && awk -v v="$v" 'BEGIN { exit !(v > 0) }'
}

log "S1: Kafka listo"
check "StatefulSet kafka y kafka-exporter listos" kafka_ready
check "topics r1/r2.energy-matched: 6 particiones, replicación 1, retention.ms=604800000, min.insync.replicas=1" topic_config
check "imagen kafka-probe construida y cargada en kind" build_probe_image
ensure_ns "$NS" >/dev/null

log "S1: produce, consume grupo A, replay con grupo B"
check "produce $COUNT eventos (acks=all, idempotente) desde un Job con sidecar" produce_ok
check "el Job termina solo con sidecar nativo (pod Succeeded)" job_terminated_alone
check "grupo A consume $COUNT/$COUNT (count y distinct)" consume_ok s01-consume-a "$group_a"
check "grupo B nuevo desde earliest consume $COUNT/$COUNT (replay del log retenido)" consume_ok s01-consume-b "$group_b"

log "S1: métricas del mesh en Prometheus"
check "istio_tcp_connections_opened_total hacia kafka con mutual_tls > 0" retry 120 tcp_mtls_series
check "esa conexión nace en $NS con mutual_tls" retry 60 tcp_mtls_from_job

k -n voltera-infra top pods >"$out/top-voltera-infra.txt" 2>&1 || true
finish "$NS"
