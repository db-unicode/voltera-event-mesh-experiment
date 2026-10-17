#!/bin/sh
# S7: vista Redis con TTL, descarte de versiones viejas y recuperación. Imprime PASS o FAIL.
# ATENCIÓN: el probe ejecuta FLUSHALL sobre market-view (la vista es efímera por diseño).
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

NS=s07-view

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s07/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
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

wait_job() {
  elapsed=0
  while [ "$elapsed" -lt 180 ]; do
    state=$(k -n "$NS" get job view-probe -o json | jq -r '
      if (.status.succeeded // 0) > 0 then "succeeded"
      elif (.status.failed // 0) > 0 then "failed"
      else "running" end')
    [ "$state" = running ] || { echo "$state"; return 0; }
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo timeout
}

log "S7: preparación"
check "market-view disponible" k -n voltera-r1 wait --for=condition=Available deployment/market-view --timeout=60s
check "imagen voltera/view-probe:dev construida y cargada en kind" sh -c "cd '$ROOT' && mise run images:build view-probe"

k apply -f "$HERE/namespace.yaml" >/dev/null
k -n "$NS" delete job view-probe --ignore-not-found --wait=true >/dev/null
k -n voltera-r1 get secret market-view-credentials -o json \
  | jq '{apiVersion, kind, type, data, metadata: {name: .metadata.name, namespace: "'"$NS"'"}}' \
  | k apply -f - >/dev/null
k apply -f "$HERE/job.yaml" >/dev/null

log "S7: ejecutando el probe (~25 s, incluye esperar la expiración de 10 s)"
state=$(wait_job)
k -n "$NS" logs job/view-probe -c view-probe >"$out/probe.log" 2>&1 || true
grep '^CHECK ' "$out/probe.log" >"$out/probe-checks.txt" || true

check "el Job del probe termina" test "$state" = succeeded
while IFS= read -r line; do
  status=$(printf '%s' "$line" | cut -d' ' -f2)
  desc=$(printf '%s' "$line" | cut -d' ' -f3-)
  n=$((n + 1))
  if [ "$status" = PASS ]; then
    echo "PASS  $desc" | tee -a "$out/result.txt"
  else
    echo "FAIL  $desc" | tee -a "$out/result.txt"
    failures=$((failures + 1))
  fi
done <"$out/probe-checks.txt"
check "el probe ejecutó los 9 checks" test "$(wc -l <"$out/probe-checks.txt" | tr -d ' ')" -eq 9

if [ "$failures" -eq 0 ]; then
  k delete namespace "$NS" --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
