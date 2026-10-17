#!/bin/sh
# S6: outbox transaccional con relay Go y Postgres. Imprime PASS o FAIL.
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

NS=s06-outbox
ROWS=1000

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s06/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
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

psql_s06() { k -n "$NS" exec s06-db-0 -c postgres -- psql -U voltera -d voltera -Atqc "$1"; }

# eq <esperado> <comando...>: el comando debe imprimir exactamente el valor esperado
eq() {
  want=$1
  shift
  got=$("$@")
  echo "esperado=$want obtenido=$got"
  [ "$got" = "$want" ]
}

# gt <mínimo> <comando...>: el valor impreso debe ser > mínimo
gt() {
  min=$1
  shift
  got=$("$@")
  echo "mínimo(>)=$min obtenido=$got"
  [ "$got" -gt "$min" ]
}

# Estado de un Job: succeeded|failed|running
job_state() {
  k -n "$NS" get job "$1" -o json | jq -r '
    if (.status.succeeded // 0) > 0 then "succeeded"
    elif ([.status.conditions[]? | select(.type == "Failed" and .status == "True")] | length) > 0 then "failed"
    else "running" end'
}

wait_job() {
  elapsed=0
  while [ "$elapsed" -lt 240 ]; do
    case $(job_state "$1") in
      succeeded) return 0 ;;
      failed) return 1 ;;
    esac
    sleep 3
    elapsed=$((elapsed + 3))
  done
  return 1
}

restarts() {
  k -n "$NS" get pod -l job-name="$1" -o json \
    | jq -r '[.items[].status.containerStatuses[]? | select(.name == "outbox-probe") | .restartCount] | add // 0'
}

last_exit() {
  k -n "$NS" get pod -l job-name="$1" -o json \
    | jq -r '[.items[].status.containerStatuses[]? | select(.name == "outbox-probe") | .lastState.terminated.exitCode // empty] | first // "none"'
}

crashed_and_restarted() {
  r=$(restarts "$1")
  e=$(last_exit "$1")
  echo "restartCount=$r lastExitCode=$e"
  [ "$r" -ge 1 ] && [ "$e" = 137 ]
}

# scenario <nombre> <failpoints> <crash:yes|no> <duplicados:yes|no>
scenario() {
  name=$1
  fps=$2
  crash=$3
  dups=$4
  job=outbox-probe-$name
  log "escenario $name (FAILPOINTS='$fps')"

  k -n "$NS" delete job "$job" --ignore-not-found --wait=true >/dev/null
  psql_s06 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;' >/dev/null 2>&1
  sed -e "s|__NAME__|$job|" -e "s|__FAILPOINTS__|$fps|" "$HERE/manifests/job.yaml" | k apply -f - >/dev/null

  if wait_job "$job"; then state=ok; else state=fail; fi
  k -n "$NS" logs "job/$job" -c outbox-probe >"$out/$name.log" 2>&1 || true
  k -n "$NS" logs "job/$job" -c outbox-probe --previous >"$out/$name.previous.log" 2>&1 || true
  k -n "$NS" get pod -l job-name="$job" -o json | jq '[.items[].status | {init: .initContainerStatuses, app: .containerStatuses}]' >"$out/$name.pod.json" 2>&1 || true
  psql_s06 "SELECT json_build_object('business',(SELECT count(*) FROM business),'outbox',(SELECT count(*) FROM outbox),'pending',(SELECT count(*) FROM outbox WHERE published_at IS NULL),'sink_total',(SELECT count(*) FROM sink),'sink_distinct',(SELECT count(DISTINCT event_id) FROM sink))" >"$out/$name.db.json" 2>&1 || true

  check "$name: el Job termina bien" test "$state" = ok
  if [ "$crash" = yes ]; then
    check "$name: el pod murió con exit 137 y Kubernetes lo reinició" crashed_and_restarted "$job"
  else
    check "$name: sin failpoint no hay reinicios" eq 0 restarts "$job"
  fi
  check "$name: $ROWS filas de negocio" eq "$ROWS" psql_s06 "SELECT count(*) FROM business"
  check "$name: $ROWS eventos en la outbox" eq "$ROWS" psql_s06 "SELECT count(*) FROM outbox"
  check "$name: pending = 0" eq 0 psql_s06 "SELECT count(*) FROM outbox WHERE published_at IS NULL"
  check "$name: count(distinct event_id) en sink = $ROWS" eq "$ROWS" psql_s06 "SELECT count(DISTINCT event_id) FROM sink"
  if [ "$dups" = yes ]; then
    check "$name: hay duplicados en sink (al-menos-una-vez)" gt 0 psql_s06 "SELECT count(*) - count(DISTINCT event_id) FROM sink"
  else
    check "$name: sin duplicados en sink" eq 0 psql_s06 "SELECT count(*) - count(DISTINCT event_id) FROM sink"
  fi
}

log "S6: preparación"
check "imagen voltera/outbox-probe:dev construida y cargada en kind" sh -c "cd '$ROOT' && mise run images:build outbox-probe"
k apply -f "$HERE/manifests/namespace.yaml" >/dev/null
k apply -k "$HERE/manifests" >/dev/null
check "Postgres del spike Ready" k -n "$NS" rollout status statefulset/s06-db --timeout=180s

scenario control "" no no
scenario commit-publish "after_commit_before_publish=crash-after:200" yes no
scenario publish-mark "after_publish_before_mark=crash-after:250" yes yes

if [ "$failures" -eq 0 ]; then
  k delete namespace "$NS" --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
