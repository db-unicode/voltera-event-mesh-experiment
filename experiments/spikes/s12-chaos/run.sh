#!/bin/sh
# S12: primitivas de caos con kubectl sobre un httpbin de prueba. Imprime PASS o FAIL.
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"
# shellcheck source=experiments/lib/expctl.sh
. "$HERE/../../lib/expctl.sh"
# shellcheck source=experiments/lib/chaos.sh
. "$HERE/../../lib/chaos.sh"

need kubectl
need jq
need git
need go
require_cluster
expctl_build

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s12/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
mkdir -p "$out/checks"
log "evidencia en ${out#"$ROOT"/}"

NS=s12-chaos
CLIENT_NS=s12-client
n=0
failures=0
: >"$out/result.txt"

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

retry() {
  limit=$1
  shift
  elapsed=0
  until "$@"; do
    [ "$elapsed" -lt "$limit" ] || return 1
    sleep 1
    elapsed=$((elapsed + 1))
  done
}

cleanup() {
  chaos_heal >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers -----------------------------------------------------------------

pod_of() { k -n "$NS" get pods -l "app=$1" -o jsonpath='{.items[0].metadata.name}'; }
spec_replicas() { k -n "$NS" get deploy "$1" -o jsonpath='{.spec.replicas}'; }
pods_of() { k -n "$NS" get pods -l "app=$1" -o name | wc -l | tr -d ' '; }
client_code() {
  k -n "$CLIENT_NS" exec deploy/curl -c curl -- \
    curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://httpbin.s12-chaos/status/200 || true
}
env_failpoints() {
  k -n "$NS" get deploy httpbin -o json \
    | jq -r '.spec.template.spec.containers[0].env // [] | map(select(.name == "FAILPOINTS")) | .[0].value // ""'
}

both_ready() { wait_ready "$NS" httpbin 180 && wait_ready "$NS" worker 180; }
worker_is() { [ "$(spec_replicas worker)" = "$1" ]; }
client_is() { [ "$(client_code)" = "$1" ]; }
no_chaos_policies() { [ "$(k get authorizationpolicy -A -l chaos=partition -o name | wc -l | tr -d ' ')" -eq 0 ]; }

# ---- despliegue ------------------------------------------------------------

log "S12: despliegue en $NS"
# una corrida anterior puede haber dejado los namespaces terminando
k delete ns "$NS" "$CLIENT_NS" --ignore-not-found --wait=true >/dev/null
k apply -f "$HERE/manifests.yaml" >/dev/null
check "a. httpbin y worker llegan a Ready con sidecar" both_ready

# ---- kill -> Ready -----------------------------------------------------------

old=$(pod_of httpbin)
t0=$(expctl now)
chaos_kill "$NS" httpbin
wait_ready "$NS" httpbin 120 || true
t1=$(expctl now)
kill_ready_s=$(expctl elapsed "$t0" "$t1")
new=$(pod_of httpbin)
log "kill -> Ready: ${kill_ready_s}s ($old -> $new)"
printf '{"kill_to_ready_s":%s,"oldPod":"%s","newPod":"%s","t0":"%s","t1":"%s"}\n' \
  "$kill_ready_s" "$old" "$new" "$t0" "$t1" >"$out/kill_ready.json"
check "b. chaos_kill: el pod cambia y vuelve a Ready (${kill_ready_s}s, umbral 60 s)" \
  jq -e '.oldPod != .newPod and .kill_to_ready_s > 0 and .kill_to_ready_s < 60' "$out/kill_ready.json"

# ---- down / up ---------------------------------------------------------------

chaos_down "$NS" httpbin
check "c. chaos_down deja 0 pods" test "$(pods_of httpbin)" -eq 0
chaos_up "$NS" httpbin 1
check "d. chaos_up 1 + wait_ready" wait_ready "$NS" httpbin 120

# ---- KEDA --------------------------------------------------------------------

chaos_pause_keda "$NS" worker 0
check "e. chaos_pause_keda a 0: el worker baja a 0 réplicas" retry 60 worker_is 0
chaos_resume_keda "$NS" worker
check "f. chaos_resume_keda: KEDA devuelve el worker a 2 réplicas" retry 90 worker_is 2
check "f2. worker 2/2 Ready tras reanudar" wait_ready "$NS" worker 120

# ---- failpoint -----------------------------------------------------------------

# go-httpbin no tiene /admin/failpoint: /post acepta POST y prueba la ruta HTTP vía expctl y el sidecar
CHAOS_OPS_PORT=8080 CHAOS_FAILPOINT_PATH=/post chaos_failpoint "$NS" httpbin after_commit_before_publish=crash-once >"$out/failpoint-http.log" 2>&1
check "g. chaos_failpoint por HTTP (POST vía expctl, mTLS STRICT) sin tocar el deployment" \
  sh -c "grep -q '(HTTP)' '$out/failpoint-http.log' && ! grep -q fallback '$out/failpoint-http.log'"
check "g2. el deployment no recibió FAILPOINTS en la ruta HTTP" test -z "$(env_failpoints)"
CHAOS_OPS_PORT=8080 CHAOS_FAILPOINT_PATH=/no-existe chaos_failpoint "$NS" httpbin before_ack=crash-after:100 >"$out/failpoint-fallback.log" 2>&1
check "h. chaos_failpoint cae a kubectl set env FAILPOINTS= si el endpoint no existe" \
  test "$(env_failpoints)" = "before_ack=crash-after:100"
check "h2. tras el fallback el deployment vuelve a Ready" wait_ready "$NS" httpbin 120

# ---- partición ---------------------------------------------------------------

check "i. antes de la partición el cliente recibe 200" retry 30 client_is 200
t0=$(expctl now)
chaos_partition "$CLIENT_NS" "$NS" 8080
check "j. chaos_partition: el cliente recibe 403 (RBAC: access denied)" retry 40 client_is 403
log "partición efectiva tras $(expctl elapsed "$t0") s"
chaos_heal
check "k. chaos_heal: el cliente vuelve a recibir 200" retry 40 client_is 200
check "l. chaos_heal no deja AuthorizationPolicies de caos" no_chaos_policies

# ---- resultado ---------------------------------------------------------------

if [ "$failures" -eq 0 ]; then
  k delete ns "$NS" "$CLIENT_NS" --wait=false >/dev/null
  echo PASS | tee -a "$out/result.txt"
  exit 0
fi
log "namespaces $NS y $CLIENT_NS conservados para depurar"
echo FAIL | tee -a "$out/result.txt"
exit 1
