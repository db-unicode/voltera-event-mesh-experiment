#!/bin/sh
# S16: lado de escritura de punta a punta en r1 (meter-sim -> RabbitMQ -> declaration-gateway -> positions-db + outbox
# -> RabbitMQ -> market-projector -> Redis) y escalado KEDA del gateway. Imprime PASS o FAIL por check y PASS/FAIL final.
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"
# shellcheck source=experiments/lib/metersim.sh
. "$HERE/../../lib/metersim.sh"

need kubectl
need jq
need curl
need git
need go
require_cluster

REGION=r1
NS=voltera-$REGION
SO=declaration-gateway
PF_GW=19610            # port-forward al /metrics del pod del gateway
PF_MGMT=19611          # port-forward al management API de RabbitMQ (profundidad de cola)
PF_PROM=19612          # port-forward a Prometheus
C_POD=${C_POD:-46}     # msgs/s de una réplica con 20 ms de proceso (medido en S5)
SPIKE_S=${SPIKE_S:-60}
SCALE_IN_LIMIT_S=${SCALE_IN_LIMIT_S:-300}
LAG_P95_LIMIT_S=${LAG_P95_LIMIT_S:-5}

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s16/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
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

# retry <segundos> <comando...>: reintenta cada 2 s hasta que el comando pase
retry() {
  limit=$1
  shift
  elapsed=0
  until "$@"; do
    [ "$elapsed" -lt "$limit" ] || return 1
    sleep 2
    elapsed=$((elapsed + 2))
  done
}

# until_eq <segundos> <esperado> <comando...>: reintenta cada 2 s hasta que la salida del comando sea el valor esperado
until_eq() {
  _limit=$1
  _want=$2
  shift 2
  _e=0
  while :; do
    _got=$("$@" 2>/dev/null || true)
    [ "$_got" != "$_want" ] || return 0
    [ "$_e" -lt "$_limit" ] || { echo "esperado $_want, actual ${_got:-<vacío>}"; return 1; }
    sleep 2
    _e=$((_e + 2))
  done
}

PF_GW_PID=
PF_MGMT_PID=
PF_PROM_PID=
SAMPLER_PID=
cleanup() {
  for p in "$SAMPLER_PID" "$PF_GW_PID" "$PF_MGMT_PID" "$PF_PROM_PID"; do
    [ -z "$p" ] || kill "$p" 2>/dev/null || true
  done
  # nunca dejar el ScaledObject pausado
  k -n "$NS" annotate scaledobject "$SO" autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# ---- accesos ----

psql_q() { k -n "$NS" exec positions-db-0 -c postgres -- psql -U voltera -d voltera -Atc "$1"; }
rds() { k -n "$NS" exec deploy/market-view -c redis -- redis-cli "$@"; }
rds_count() { rds --scan --pattern "$1" | wc -l | tr -d ' '; }
# rds_versions <patrón>: "key versión" ordenado, una línea por key
rds_versions() {
  k -n "$NS" exec deploy/market-view -c redis -- sh -c \
    "redis-cli --scan --pattern '$1' | sort | while read -r key; do echo \"\$key \$(redis-cli hget \"\$key\" _v)\"; done"
}
# rds_ttls <patrón>: TTL en segundos de cada key
rds_ttls() {
  k -n "$NS" exec deploy/market-view -c redis -- sh -c \
    "redis-cli --scan --pattern '$1' | while read -r key; do redis-cli ttl \"\$key\"; done"
}

count_positions() { psql_q "select count(*) from positions where meter_id like '$1'"; }
outbox_pending() { psql_q "select count(*) from outbox where published_at is null"; }

gw_metrics() { curl -fsS -m 5 "http://127.0.0.1:$PF_GW/metrics"; }
# gw_sum <métrica> [regex de filtro]: suma de la métrica (todas las series que cumplan el filtro) en el pod del gateway
gw_sum() {
  gw_metrics | awk -v n="$1" -v f="${2:-.}" '$1 ~ ("^" n "({|$)") && $0 ~ f { s += $NF } END { printf "%d\n", s }'
}

KEDA_PW=$(k -n voltera-infra get secret rabbitmq-users -o json | jq -er '.data.keda' | base64 -d)
depth() { curl -fsS -m 5 -u "keda:$KEDA_PW" "http://127.0.0.1:$PF_MGMT/api/queues/$REGION/meter-declarations" | jq -r '.messages'; }
replicas() { k -n "$NS" get deploy declaration-gateway -o jsonpath='{.status.replicas}'; }
prom_q() { curl -fsS -m 10 -G "http://127.0.0.1:$PF_PROM/api/v1/query" --data-urlencode "query=$1"; }

pf() { # <ns> <target> <local:remote> <archivo de log>; imprime el PID
  kubectl --context "$KUBE_CONTEXT" -n "$1" port-forward "$2" "$3" >"$4" 2>&1 &
  echo $!
}

# ---- despliegue y estado inicial ----

deploy_write_side() {
  [ "${SKIP_DEPLOY:-0}" = 1 ] && return 0
  MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/apps/write-side" "$REGION"
}

# pin_gateway: KEDA mantiene exactamente 1 réplica (annotation oficial de pausa) para leer sus contadores
pin_gateway() {
  k -n "$NS" annotate scaledobject "$SO" autoscaling.keda.sh/paused-replicas=1 --overwrite || return 1
  until_eq 150 1 ready_replicas
}

ready_replicas() { k -n "$NS" get deploy declaration-gateway -o jsonpath='{.status.readyReplicas}'; }

ready_all() {
  k -n "$NS" wait --for=condition=Ready pod/positions-db-0 --timeout=120s &&
    k -n "$NS" rollout status deploy/market-view --timeout=120s &&
    k -n "$NS" rollout status deploy/market-projector --timeout=120s &&
    k -n "$NS" rollout status deploy/price-feed-simulator --timeout=120s
}

forwards_ready() {
  depth >/dev/null && curl -fsS -m 5 "http://127.0.0.1:$PF_PROM/-/ready" >/dev/null
}

gw_forward() {
  [ -z "$PF_GW_PID" ] || kill "$PF_GW_PID" 2>/dev/null || true
  pod=$(k -n "$NS" get pod -l app=declaration-gateway -o jsonpath='{.items[0].metadata.name}')
  PF_GW_PID=$(pf "$NS" "pod/$pod" "$PF_GW:9090" "$out/port-forward-gw.log")
  retry 30 gw_metrics
}

clean_data() {
  psql_q "DELETE FROM outbox WHERE aggregate_id LIKE 's16%'; DELETE FROM positions WHERE meter_id LIKE 's16%'" || return 1
  k -n "$NS" exec deploy/market-view -c redis -- sh -c \
    "redis-cli --scan --pattern 'pos:*:s16*' | xargs -r redis-cli del >/dev/null"
  [ "$(rds_count 'pos:*:s16*')" = 0 ]
}

# ---- (a) alta de 200 medidores de dos fabricantes ----

check_a() {
  proc0=$(gw_sum declarations_processed_total)
  metersim_run "$REGION" --profile once --meters 200 --prefix s16- --seed 16 --rate 200 >"$out/once.log" 2>&1 || return 1
  metersim_summary <"$out/once.log" >"$out/once-summary.json"
  jq -e '.published == 200 and .errors == 0 and .byVendor["acme-v1"] == 100 and .byVendor["voltix-2"] == 100' "$out/once-summary.json" || return 1
  until_eq 90 200 count_positions 's16-%' || return 1
  echo "positions-db: $(psql_q "select count(*) from positions where meter_id like 's16-%'") filas s16-"
  psql_q "select zone, count(*) from positions where meter_id like 's16-%' group by 1 order by 1"
  until_eq 60 200 rds_count 'pos:*:s16-*' || return 1
  echo "redis: $(rds_count 'pos:*:s16-*') keys pos:*:s16-*"
  for z in 1 2 3 4; do
    c=$(rds_count "pos:z-$z:s16-*")
    echo "zona z-$z: $c keys"
    [ "$c" = 50 ] || return 1
  done
  rds_ttls 'pos:*:s16-*' | sort -n >"$out/ttls.txt"
  lo=$(head -n 1 "$out/ttls.txt")
  hi=$(tail -n 1 "$out/ttls.txt")
  echo "TTL de las keys: mínimo ${lo}s, máximo ${hi}s (esperado ~150)"
  [ "$lo" -ge 100 ] && [ "$hi" -le 150 ] && [ "$hi" -ge 140 ] || return 1
  fields=$(rds hmget pos:z-1:s16-0004 meterId side kwh validUntil zone region eventId occurredAt committedAt projectedAt)
  echo "$fields"
  [ "$(echo "$fields" | grep -c .)" = 10 ] || return 1
  proc1=$(gw_sum declarations_processed_total)
  echo "declarations_processed_total: $proc0 -> $proc1"
  [ $((proc1 - proc0)) = 200 ] || return 1
  until_eq 30 0 outbox_pending
}

# ---- (b) reenvío del mismo lote ----

stale_delta() { echo $(($(gw_sum events_stale_dropped_total) - stale0)); }
rejected_delta() { echo $(($(gw_sum declarations_rejected_total 'reason="unknown_vendor"') - rej0)); }

snap_db() { psql_q "select md5(string_agg(meter_id || ':' || version || ':' || updated_at::text, ',' order by meter_id)) from positions where meter_id like 's16-%'"; }
snap_outbox() { psql_q "select count(*) from outbox where aggregate_id like 's16-%'"; }

check_b() {
  db0=$(snap_db)
  ob0=$(snap_outbox)
  rd0=$(rds_versions 'pos:*:s16-*' | cksum)
  stale0=$(gw_sum events_stale_dropped_total)
  proc0=$(gw_sum declarations_processed_total)
  metersim_run "$REGION" --profile dup --dup-k 1 --meters 200 --prefix s16- --seed 16 --rate 200 >"$out/dup.log" 2>&1 || return 1
  metersim_summary <"$out/dup.log" >"$out/dup-summary.json"
  jq -e '.published == 200 and .errors == 0' "$out/dup-summary.json" || return 1
  until_eq 60 200 stale_delta || return 1
  stale1=$(gw_sum events_stale_dropped_total)
  proc1=$(gw_sum declarations_processed_total)
  echo "events_stale_dropped_total: $stale0 -> $stale1 (+$((stale1 - stale0)))"
  echo "declarations_processed_total: $proc0 -> $proc1"
  [ $((stale1 - stale0)) = 200 ] && [ $((proc1 - proc0)) = 200 ] || return 1
  sleep 3
  db1=$(snap_db)
  ob1=$(snap_outbox)
  rd1=$(rds_versions 'pos:*:s16-*' | cksum)
  echo "positions-db (versiones y updated_at): $db0 -> $db1"
  echo "outbox s16-: $ob0 -> $ob1"
  echo "redis (key, versión): $rd0 -> $rd1; keys: $(rds_count 'pos:*:s16-*')"
  [ "$db0" = "$db1" ] && [ "$ob0" = "$ob1" ] && [ "$rd0" = "$rd1" ] && [ "$(rds_count 'pos:*:s16-*')" = 200 ]
}

# ---- (c) formato desconocido ----

check_c() {
  rej0=$(gw_sum declarations_rejected_total 'reason="unknown_vendor"')
  err0=$(gw_sum events_consumed_total 'result="error"')
  metersim_run "$REGION" --profile once --meters 1 --prefix s16bad- --seed 16 --rate 0 --vendor mystery-v3 >"$out/bad.log" 2>&1 || return 1
  until_eq 30 1 rejected_delta || return 1
  sleep 8   # un requeue erróneo volvería a contar
  rej1=$(gw_sum declarations_rejected_total 'reason="unknown_vendor"')
  err1=$(gw_sum events_consumed_total 'result="error"')
  echo "declarations_rejected_total{reason=unknown_vendor}: $rej0 -> $rej1"
  echo "events_consumed_total{result=error}: $err0 -> $err1"
  gw_metrics | grep -E '^(declarations_rejected_total|events_consumed_total\{.*result="error")' || true
  [ $((rej1 - rej0)) = 1 ] && [ $((err1 - err0)) = 1 ] || return 1
  echo "positions s16bad-: $(psql_q "select count(*) from positions where meter_id like 's16bad-%'"), redis: $(rds_count 'pos:*:s16bad-*')"
  [ "$(psql_q "select count(*) from positions where meter_id like 's16bad-%'")" = 0 ] && [ "$(rds_count 'pos:*:s16bad-*')" = 0 ] || return 1
  # la cola quedó vacía: se descartó, no se reencola
  [ "$(depth)" = 0 ]
}

# ---- (d) precio ----

price_version() { rds hget "price:$REGION" _v; }
price_changed() { if [ "$(price_version)" != "$v0" ]; then echo yes; else echo no; fi; }

check_d() {
  v0=$(price_version)
  [ -n "$v0" ] || return 1
  echo "price:$REGION inicial: $(rds hmget "price:$REGION" pricePerKwh validUntil eventId | tr '\n' ' ') (_v=$v0, ttl $(rds ttl "price:$REGION")s)"
  until_eq 45 yes price_changed || return 1
  v1=$(price_version)
  echo "price:$REGION actualizado: $(rds hmget "price:$REGION" pricePerKwh validUntil eventId | tr '\n' ' ') (_v=$v1, ttl $(rds ttl "price:$REGION")s)"
  [ "$v1" -gt "$v0" ] && [ "$(rds ttl "price:$REGION")" -gt 0 ]
}

# ---- (f) el binario del proyector no enlaza pgx ----

check_f() {
  (cd "$ROOT" && go build -trimpath -ldflags='-s -w' -o "$out/market-projector" ./cmd/market-projector) || return 1
  mods=$(go version -m "$out/market-projector" | grep -c 'jackc/pgx' || true)
  deps=$(cd "$ROOT" && go list -deps ./cmd/market-projector | grep -c pgx || true)
  echo "go version -m | grep -c jackc/pgx = $mods; go list -deps ./cmd/market-projector | grep -c pgx = $deps"
  go version -m "$out/market-projector" | grep -E 'dep|mod' | awk '{print $2}' | sort >"$out/projector-modules.txt"
  [ "$mods" = 0 ] && [ "$deps" = 0 ]
}

# ---- (g) latencia committed -> vista ----

LAG_Q='sum by (le) (increase(event_e2e_lag_seconds_bucket{service="market-projector",event_type="PositionDeclared",stage="committed",namespace="voltera-r1"}[10m]))'

lag_p95() {
  label=$1
  prom_q "histogram_quantile(0.95, $LAG_Q)" >"$out/prom-p95-$label.json" || return 1
  prom_q "histogram_quantile(0.50, $LAG_Q)" >"$out/prom-p50-$label.json" || return 1
  prom_q "sum(increase(event_e2e_lag_seconds_count{service=\"market-projector\",event_type=\"PositionDeclared\",stage=\"committed\",namespace=\"voltera-r1\"}[10m]))" >"$out/prom-count-$label.json" || return 1
  p95=$(jq -r '.data.result[0].value[1] // "NaN"' "$out/prom-p95-$label.json")
  p50=$(jq -r '.data.result[0].value[1] // "NaN"' "$out/prom-p50-$label.json")
  cnt=$(jq -r '.data.result[0].value[1] // "0"' "$out/prom-count-$label.json")
  echo "committed -> vista ($label): p50=${p50}s p95=${p95}s sobre ~${cnt} eventos (ventana 10 min, histogram_quantile sobre event_e2e_lag_seconds)"
  echo "$p95" >"$out/lag-p95-$label.txt"
  awk -v v="$p95" -v lim="$LAG_P95_LIMIT_S" 'BEGIN { exit !(v != "NaN" && v + 0 > 0 && v + 0 < lim) }'
}

lag_p95_after_a() {
  sleep 20   # Prometheus raspa cada 15 s
  retry 60 lag_p95 after-batch
}

# ---- (e) KEDA: 0 -> N -> 0 ----

unpin_gateway() {
  k -n "$NS" annotate scaledobject "$SO" autoscaling.keda.sh/paused-replicas- || return 1
  retry 180 at_rest_zero
}

at_rest_zero() { [ "$(replicas)" = "" ] || [ "$(replicas)" = 0 ]; }

sampler() { # <archivo>: ts,replicas,depth cada 2 s
  echo "ts,replicas,depth" >"$1"
  while :; do
    r=$(replicas 2>/dev/null || true)
    d=$(depth 2>/dev/null || true)
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),${r:-0},${d:-}" >>"$1"
    sleep 2
  done
}

run_spike() {
  rate=$((6 * C_POD))
  echo "spike: rate=$rate/s (6 x C_pod=$C_POD) durante ${SPIKE_S}s, rampa 5 s, 500 medidores"
  METERSIM_DEADLINE=$((SPIKE_S + 180)) metersim_run "$REGION" --profile spike --rate "$rate" --duration "$SPIKE_S" --ramp 5 \
    --meters 500 --prefix s16e- --seed 17 >"$out/spike.log" 2>&1 || return 1
  metersim_summary <"$out/spike.log" >"$out/spike-summary.json"
  cat "$out/spike-summary.json"
  jq -e '.errors == 0 and .published > 0' "$out/spike-summary.json"
}

wait_scale_in() {
  t0=$(date +%s)
  while :; do
    last=$(tail -n 1 "$out/samples.csv")
    case $last in
      *,0,0) return 0 ;;
    esac
    [ $(($(date +%s) - t0)) -lt "$SCALE_IN_LIMIT_S" ] || { echo "sin volver a 0 en ${SCALE_IN_LIMIT_S}s; última muestra: $last"; return 1; }
    sleep 3
  done
}

analyze_samples() {
  awk -F, '
    NR == 1 { next }
    NR == 2 { first_r = $2 }
    { if ($2 > max) max = $2; if ($3 + 0 > dmax) dmax = $3 + 0; last_r = $2; last_d = $3 }
    END {
      printf "primera_muestra_replicas=%s pico_replicas=%s pico_profundidad=%s final_replicas=%s final_profundidad=%s\n", first_r, max, dmax, last_r, last_d
      exit !(first_r == 0 && max >= 2 && max <= 10 && dmax > 0 && last_r == 0 && last_d == 0)
    }' "$out/samples.csv"
}

check_after_spike() {
  until_eq 60 0 outbox_pending || return 1
  until_eq 60 500 count_positions 's16e-%' || return 1
  echo "outbox: $(psql_q "select count(*) from outbox where aggregate_id like 's16e-%'") eventos s16e-, 0 pendientes; positions s16e-: 500"
  until_eq 60 500 rds_count 'pos:*:s16e-*' || return 1
  echo "redis: 500 keys pos:*:s16e-*"
}

# ---------------------------------------------------------------------------------------------

log "S16: despliegue y requisitos"
check "lado de escritura desplegado en $REGION (mise run apps:write-side $REGION)" deploy_write_side
check "positions-db, market-view, market-projector y price-feed-simulator Ready" ready_all
PF_MGMT_PID=$(pf voltera-infra pod/rabbitmq-0 "$PF_MGMT:15672" "$out/port-forward-mgmt.log")
PF_PROM_PID=$(pf istio-system svc/prometheus "$PF_PROM:9090" "$out/port-forward-prom.log")
check "management API de RabbitMQ y Prometheus por port-forward" retry 30 forwards_ready
check "gateway fijado en 1 réplica (ScaledObject pausado) para leer sus contadores" pin_gateway
check "port-forward al /metrics del pod del gateway" gw_forward
check "datos de corridas anteriores (s16*) eliminados de positions-db, outbox y Redis" clean_data

log "S16: (a) 200 medidores, mitad acme-v1 y mitad voltix-2"
check "(a) once con 200 medidores => 200 posiciones en positions-db, 200 keys pos:* en Redis con TTL ~150 s, 50 por zona" check_a
log "S16: (g) latencia tras el lote"
check "(g) latencia committed->vista p95 < ${LAG_P95_LIMIT_S}s desde Prometheus (tras el lote de 200)" lag_p95_after_a
log "S16: (b) reenvío del lote"
check "(b) dup del mismo lote => positions-db y Redis sin cambios, stale dropped = 200, sin eventos nuevos en la outbox" check_b
log "S16: (c) formato desconocido"
check "(c) x-vendor desconocido => rechazado y contado, no llega a Redis ni a positions-db" check_c
log "S16: (d) precio"
check "(d) price:$REGION presente y se actualiza" check_d
log "S16: (f) binario del proyector"
check "(f) el binario de market-projector no contiene pgx" check_f

log "S16: (e) escalado KEDA"
check "(e) gateway libre de la pausa y en 0 réplicas en reposo" unpin_gateway
sampler "$out/samples.csv" &
SAMPLER_PID=$!
sleep 4
check "(e) spike de 6 x C_pod ($((6 * C_POD)) msgs/s) durante ${SPIKE_S}s" run_spike
check "(e) vuelve a 0 réplicas con la cola en 0 (<= ${SCALE_IN_LIMIT_S}s)" wait_scale_in
sleep 4
kill "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=
check "(e) serie de réplicas y profundidad: 0 -> >= 2 (<= 10) -> 0" analyze_samples
check "(e) tras el scale-in: outbox sin pendientes, 500 posiciones en positions-db y 500 keys en Redis" check_after_spike
check "(g) latencia committed->vista p95 < ${LAG_P95_LIMIT_S}s desde Prometheus (incluye el spike)" retry 40 lag_p95 after-spike

k -n "$NS" top pods >"$out/top-pods.txt" 2>&1 || true
k -n "$NS" get scaledobject,hpa,deploy,pods -o wide >"$out/k8s-state.txt" 2>&1 || true
k -n "$NS" get events --sort-by=.lastTimestamp >"$out/events.txt" 2>&1 || true
k -n "$NS" get scaledobject "$SO" -o yaml >"$out/scaledobject.yaml" 2>&1 || true
k -n "$NS" describe hpa >"$out/hpa-describe.txt" 2>&1 || true
k -n "$NS" logs deploy/market-projector -c market-projector --tail=50 >"$out/projector-tail.log" 2>&1 || true

if [ "$failures" -eq 0 ]; then
  clean_data >/dev/null 2>&1 || true
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
