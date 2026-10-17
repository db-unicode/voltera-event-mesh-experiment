#!/bin/sh
# S0: plataforma base (kind + Istio + KEDA + observabilidad). Imprime PASS o FAIL.
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"

need kubectl
need kind
need jq
need git
need docker

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s00/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
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

svc_get() { k get --raw "/api/v1/namespaces/$1/services/$2/proxy/$3"; }

nodes_ready() {
  k get nodes -o json | jq -e '.items | length > 0 and all(.[]; .status.conditions[] | select(.type == "Ready") | .status == "True")'
}

deploy_available() { k -n "$1" wait --for=condition=Available "deployment/$2" --timeout=120s; }

keda_crd() { k get crd scaledobjects.keda.sh; }

prom_up() {
  svc_get istio-system prometheus:9090 'api/v1/query?query=up' | jq -e '.data.result | length > 0'
}

kiali_sees_prometheus() {
  svc_get istio-system kiali:20001 'kiali/api/status' \
    | jq -e '.externalServices | map(select(.name == "Prometheus")) | length > 0'
}

grafana_ok() { svc_get istio-system grafana:3000 'api/health' | jq -e '.database == "ok"'; }

jaeger_ok() { svc_get istio-system tracing:80 'jaeger/api/services' | jq -e 'has("data")'; }

# httpbin y su sidecar deben estar los dos listos (istio-proxy puede ser init container nativo)
httpbin_two_containers() {
  k -n s00-mesh get pod -l app=httpbin -o json | jq -e '
    .items[0].status
    | [(.containerStatuses // []), (.initContainerStatuses // [])] | add
    | map(select(.name == "istio-proxy" or .name == "httpbin"))
    | length == 2 and all(.[]; .ready)'
}

mtls_call_ok() {
  k -n s00-mesh exec deploy/curl -c curl -- curl -fsS --max-time 10 http://httpbin.s00-mesh/headers \
    | tee "$out/mtls-headers.json" \
    | jq -e '(."X-Forwarded-Client-Cert" // .headers["X-Forwarded-Client-Cert"] // []) | tostring | contains("spiffe://cluster.local/ns/s00-mesh/sa/curl")'
}

# control positivo: el pod sin sidecar sí tiene red (llega a Prometheus, que no está en la malla)
plain_has_network() {
  k -n s00-plain exec deploy/curl-plain -c curl -- curl -fsS --max-time 10 http://prometheus.istio-system:9090/-/ready
}

plain_rejected() {
  if k -n s00-plain exec deploy/curl-plain -c curl -- curl -sS --max-time 10 http://httpbin.s00-mesh/headers; then
    echo "el pod sin sidecar pudo llamar a httpbin: STRICT no se aplica"
    return 1
  fi
  echo "rechazado como se esperaba"
}

prom_has_httpbin_traffic() {
  svc_get istio-system prometheus:9090 'api/v1/query?query=istio_requests_total' \
    | jq -e '.data.result | map(select(.metric.destination_service_name == "httpbin")) | length > 0'
}

top_available() { k top nodes >"$out/top-nodes.txt" && k top pods -A >"$out/top-pods.txt"; }

log "S0: estado base"
check "nodo Ready" nodes_ready
check "istiod Ready" deploy_available istio-system istiod
check "KEDA operator Ready" deploy_available keda keda-operator
check "CRD scaledobjects.keda.sh existe" keda_crd
for d in prometheus kiali grafana jaeger; do
  check "$d Ready" deploy_available istio-system "$d"
done
check "Prometheus responde 'up' con series" retry 60 prom_up
check "Kiali ve Prometheus" retry 60 kiali_sees_prometheus
check "Grafana responde /api/health" grafana_ok
check "Jaeger responde la API de servicios" jaeger_ok

log "S0: pods de prueba y mTLS STRICT"
k apply -f "$HERE/manifests.yaml" >"$out/apply.log"
check "httpbin Available" deploy_available s00-mesh httpbin
check "curl (con sidecar) Available" deploy_available s00-mesh curl
check "curl-plain (sin sidecar) Available" deploy_available s00-plain curl-plain
check "httpbin tiene 2 contenedores listos (app + istio-proxy)" httpbin_two_containers
check "llamada con sidecar funciona y presenta identidad SPIFFE (mTLS)" retry 30 mtls_call_ok
check "pod sin sidecar tiene red (control positivo)" plain_has_network
check "pod sin sidecar es rechazado por mTLS STRICT" plain_rejected
check "Prometheus registra istio_requests_total de httpbin" retry 120 prom_has_httpbin_traffic

log "S0: consumo de recursos"
check "metrics-server entrega kubectl top" retry 180 top_available
k get pods -A -o wide >"$out/pods.txt"
if [ -s "$out/top-pods.txt" ]; then
  awk 'NR > 1 {
    v = $4; n = v + 0
    if (v ~ /Ki$/) n = n / 1024; else if (v ~ /Gi$/) n = n * 1024; else if (v !~ /Mi$/) n = n / 1048576
    s[$1] += n; t += n
  } END {
    for (ns in s) printf "%s\t%.0f\n", ns, s[ns]
    printf "TOTAL\t%.0f\n", t
  }' "$out/top-pods.txt" | sort >"$out/ram-by-namespace-mib.tsv"
fi
docker stats --no-stream --format '{{.Name}}	{{.MemUsage}}' "${CLUSTER_NAME}-control-plane" >"$out/docker-stats-node.txt" 2>&1 || true

if [ "$failures" -eq 0 ]; then
  k delete namespace s00-mesh s00-plain --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
