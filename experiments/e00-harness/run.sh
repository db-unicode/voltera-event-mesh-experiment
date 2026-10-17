#!/bin/sh
# E00: humo del arnés (sin servicios de negocio). Se carga con `.` desde experiments/run.sh.
# Define drive y collect; usa OUT, EXPCTL y expctl del runner.

# drive: 30 s de muestreo de réplicas de kafka-exporter y de `up` de Prometheus
drive() {
  expctl sample --interval 2s --duration 30s --out "$OUT/raw/samples.csv" \
    --replicas kafka_exporter=voltera-infra/kafka-exporter \
    --prom 'up_prometheus=up{job="prometheus"}' || return 1
}

# collect <t_start> <t_end> <out>: guarda las consultas de Prometheus y metrics.json
collect() {
  t0=$1
  t1=$2
  dir=$3
  expctl prom query 'up{job="prometheus"}' --time "$t1" >"$dir/prom/up.json" || return 1
  expctl prom range 'up{job="prometheus"}' --start "$t0" --end "$t1" --step 2s >"$dir/prom/up_range.json" || return 1
  jq -n \
    --slurpfile instant "$dir/prom/up.json" \
    --slurpfile range "$dir/prom/up_range.json" \
    --arg t0 "$t0" --arg t1 "$t1" \
    '{
      tStart: $t0,
      tEnd: $t1,
      prom_up: ($instant[0].data.result[0].value[1] | tonumber),
      prom_range_points: ($range[0].data.result[0].values | length),
      prom_range_series: ($range[0].data.result | length)
    }' >"$dir/metrics.json"
}
