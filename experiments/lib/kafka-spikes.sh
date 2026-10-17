#!/bin/sh
# Helpers de los spikes S1-S3 (Kafka). Se usa con `.` después de common.sh; no se ejecuta.

PROBE_IMAGE=voltera/kafka-probe:dev
KAFKA_BOOTSTRAP=kafka.voltera-infra.svc:9092

# init_results <sNN>: crea $out (evidencia), $n/$failures y vacía result.txt
init_results() {
  sha=$(git -C "$ROOT" rev-parse --short HEAD)
  [ -z "$(git -C "$ROOT" status --porcelain)" ] || sha=${sha}-dirty
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  out="$ROOT/experiments/results/$1/$stamp-$sha"
  mkdir -p "$out/checks"
  log "evidencia en ${out#"$ROOT"/}"
  n=0
  failures=0
  : >"$out/result.txt"
}

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

# retry <segundos> <comando...>: reintenta cada 3 s hasta que el comando pase
retry() {
  limit=$1
  shift
  elapsed=0
  until "$@"; do
    [ "$elapsed" -lt "$limit" ] || return 1
    sleep 3
    elapsed=$((elapsed + 3))
  done
}

# finish: veredicto final; con PASS borra los namespaces de prueba dados
finish() {
  if [ "$failures" -eq 0 ]; then
    [ "$#" -eq 0 ] || k delete namespace "$@" --wait=false >/dev/null
    echo "PASS" | tee -a "$out/result.txt"
    exit 0
  fi
  echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
  exit 1
}

build_probe_image() {
  MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/images/build" kafka-probe
}

# kafka_ready: el StatefulSet de Kafka y el exporter están listos
kafka_ready() {
  k -n voltera-infra rollout status statefulset/kafka --timeout=5s \
    && k -n voltera-infra rollout status deployment/kafka-exporter --timeout=5s
}

# ensure_ns <ns>: namespace con inyección de sidecar
ensure_ns() {
  k apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $1
  labels: {istio-injection: enabled}
YAML
}

# probe_job <ns> <nombre> <args...>: corre kafka-probe como Job (con sidecar), espera su fin,
# guarda el log en $out/<nombre>.log y devuelve 0 si el Job completó.
probe_job() {
  pj_ns=$1
  pj_name=$2
  shift 2
  pj_args=""
  for a in "$@"; do pj_args="$pj_args\"$a\","; done
  k -n "$pj_ns" delete job "$pj_name" --ignore-not-found --wait=true >/dev/null
  k apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $pj_name
  namespace: $pj_ns
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: {app: kafka-probe}
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: $PROBE_IMAGE
          env:
            - {name: KAFKA_BROKERS, value: "$KAFKA_BOOTSTRAP"}
          args: [${pj_args%,}]
          resources:
            requests: {cpu: 50m, memory: 32Mi}
            limits: {memory: 256Mi}
YAML
  pj_elapsed=0
  while :; do
    pj_state=$(k -n "$pj_ns" get job "$pj_name" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $pj_state in
      1/*) pj_rc=0; break ;;
      */1) pj_rc=1; break ;;
    esac
    if [ "$pj_elapsed" -ge 240 ]; then
      pj_rc=1
      break
    fi
    sleep 2
    pj_elapsed=$((pj_elapsed + 2))
  done
  k -n "$pj_ns" logs "job/$pj_name" -c probe >"$out/$pj_name.log" 2>&1 || true
  k -n "$pj_ns" get pod -l "job-name=$pj_name" -o yaml >"$out/$pj_name-pod.yaml" 2>&1 || true
  cat "$out/$pj_name.log"
  return "$pj_rc"
}

# prom_query <promql>: respuesta JSON de Prometheus (por el proxy del API server)
prom_query() {
  pq=$(jq -rn --arg q "$1" '$q|@uri')
  k get --raw "/api/v1/namespaces/istio-system/services/prometheus:9090/proxy/api/v1/query?query=$pq"
}

# prom_scalar <promql>: primer valor del resultado (vacío si no hay series)
prom_scalar() {
  prom_query "$1" | jq -r '.data.result[0].value[1] // empty'
}
