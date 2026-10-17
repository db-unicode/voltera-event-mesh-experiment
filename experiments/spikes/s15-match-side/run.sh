#!/bin/sh
# S15: lado de lectura (matching-engine -> Kafka -> energy-transfer/device-sim y mesh-bridge -> NATS -> billing-sim).
# Siembra la vista Redis de r1 y r2 sin depender del lado de escritura. Imprime PASS o FAIL.
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
need curl
need openssl
require_cluster

N=${S15_N:-20}   # matches sembrados en r1
M=${S15_M:-5}    # matches sembrados en r2
NS=s15-match-side
DEDUP_WAIT=135   # > duplicate_window del stream (120 s) desde la publicación original
PF_DEVICE_R1=19510
PF_DEVICE_R2=19511
PF_BILLING=19512
PF_ENGINE_R1=19513
PF_ENGINE_R2=19514

init_results s15
run=$(date -u +%d%H%M%S)   # prefijo único por ejecución: ids de medidor, zona y matchIds no chocan
pfx_r1=s15-$run-a
pfx_r2=s15-$run-b
zone_r1=z-s15-$run-a
zone_r2=z-s15-$run-b
csv="$out/s15-replicas-r1.csv"
pids=""
rules_orig=$ROOT/deploy/apps/base/matching-engine/rules.cfg

cleanup() {
  for p in $pids; do kill "$p" 2>/dev/null || true; done
  # deja las reglas como estaban si el spike terminó a mitad de la prueba de recarga
  set_rules "$rules_orig" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

pf() { # <ns> <target> <local> <remote>
  k -n "$1" port-forward "$2" "$3:$4" >/dev/null 2>&1 &
  pids="$pids $!"
}

psql_r() { # <region> <sql>
  k -n "voltera-$1" exec matches-db-0 -c postgres -- psql -U voltera -d voltera -tAc "$2"
}

psql_transfer() { # <region> <sql>
  k -n "voltera-$1" exec transfer-db-0 -c postgres -- psql -U voltera -d voltera -tAc "$2"
}

state() { curl -fsS --max-time 5 "http://127.0.0.1:$1/state"; }

# --- preparación ---------------------------------------------------------------------------

deploy_apps() { MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/apps/match-side" all; }

start_forwards() {
  pf voltera-r1 svc/device-sim "$PF_DEVICE_R1" 8080
  pf voltera-r2 svc/device-sim "$PF_DEVICE_R2" 8080
  pf voltera-global svc/billing-sim "$PF_BILLING" 8080
  pf voltera-r1 deploy/matching-engine "$PF_ENGINE_R1" 9090
  pf voltera-r2 deploy/matching-engine "$PF_ENGINE_R2" 9090
  retry 30 state "$PF_DEVICE_R1" && retry 30 state "$PF_DEVICE_R2" && retry 30 state "$PF_BILLING" \
    && retry 30 curl -fsS "http://127.0.0.1:$PF_ENGINE_R1/readyz" && retry 30 curl -fsS "http://127.0.0.1:$PF_ENGINE_R2/readyz"
}

replicas_r1() { k -n voltera-r1 get deploy energy-transfer -o jsonpath='{.status.replicas}'; }

idle_at_zero() {
  r=$(replicas_r1)
  echo "replicas=${r:-0}"
  [ "${r:-0}" = 0 ]
}

sample_loop() {
  echo "ts,replicas" >"$csv"
  while :; do
    r=$(replicas_r1 2>/dev/null || true)
    echo "$(date -u +%H:%M:%S),${r:-0}" >>"$csv"
    sleep 2
  done
}

# --- siembra -------------------------------------------------------------------------------

# seed <region> <count> <prefix> <zone>: N oferentes + N demandantes emparejables en una zona propia y el precio.
# Escribe la vista con redis-cli (la vista real la escribe market-projector; aquí no hay lado de escritura).
seed() {
  sr=$1
  sn=$2
  sp=$3
  sz=$4
  {
    i=1
    while [ "$i" -le "$sn" ]; do
      for side in SURPLUS DEFICIT; do
        case $side in
          SURPLUS) id=$sp-s$i ;;
          *) id=$sp-d$i ;;
        esac
        echo "HSET pos:$sz:$id _v 1 meterId $id side $side kwh $((3 + i)).5 validUntil 2099-01-01T00:00:00Z zone $sz region $sr eventId s15-$id"
        echo "EXPIRE pos:$sz:$id 120"
      done
      i=$((i + 1))
    done
    # precio: versión 1 para que cualquier PriceUpdated real posterior (versión mayor) lo reemplace
    echo "DEL price:$sr"
    echo "HSET price:$sr _v 1 pricePerKwh 0.21 validUntil 2099-01-01T00:00:00Z eventId s15-price"
    echo "EXPIRE price:$sr 600"
  } | k -n "voltera-$sr" exec -i deploy/market-view -c redis -- redis-cli
}

# --- comprobaciones ------------------------------------------------------------------------

matches_created() { # <region> <count> <prefix>
  c=$(psql_r "$1" "SELECT count(*) FROM matches WHERE seller_meter_id LIKE '$3-s%'")
  echo "matches=$c esperado=$2"
  [ "$c" = "$2" ]
}

marks_ok() { # <region> <count> <prefix>: 2 posiciones marcadas por match
  c=$(psql_r "$1" "SELECT count(*) FROM matched_positions WHERE meter_id LIKE '$3-%'")
  echo "matched_positions=$c esperado=$(($2 * 2))"
  [ "$c" = "$(($2 * 2))" ]
}

outbox_pending_zero() { # <region>
  c=$(psql_r "$1" "SELECT count(*) FROM outbox WHERE published_at IS NULL")
  echo "outbox pendiente=$c"
  [ "$c" = 0 ]
}

dump_ids() { # <region> <prefix> <file>
  psql_r "$1" "SELECT match_id FROM matches WHERE seller_meter_id LIKE '$2-s%' ORDER BY 1" | jq -R . | jq -s . >"$3"
}

transfers_sent() { # <region> <count> <prefix>
  c=$(psql_transfer "$1" "SELECT count(*) FROM transfers WHERE seller_meter_id LIKE '$3-s%' AND status = 'SENT'")
  echo "transfers SENT=$c esperado=$2"
  [ "$c" = "$2" ]
}

device_has_all() { # <port> <idsfile> <count>: cada matchId con efecto exactamente una vez
  state "$1" >"$out/device-$1.json" || return 1
  jq -e --slurpfile ids "$2" '
    ($ids[0] | length) as $n
    | ($ids[0] - (.byMatchId | keys)) == []
    and ([$ids[0][] as $i | .byMatchId[$i]] | all(. == 1))
    and $n > 0' "$out/device-$1.json"
}

billing_has_all() { # <idsfile> <region>: cada matchId facturado una vez y atribuido a su región
  state "$PF_BILLING" >/dev/null && curl -fsS "http://127.0.0.1:$PF_BILLING/state?detail=1" >"$out/billing.json" || return 1
  jq -e --slurpfile ids "$1" --arg r "$2" '
    [$ids[0][] as $i | .byMatchId[$i]] | length > 0 and all(. != null and .region == $r and .count == 1)' "$out/billing.json"
}

billing_regions_split() { # <n1> <n2>
  jq -e --argjson a "$1" --argjson b "$2" '.byRegion.r1.distinct >= $a and .byRegion.r2.distinct >= $b' "$out/billing.json"
}

engine_metrics() { # <port>
  curl -fsS "http://127.0.0.1:$1/metrics" >"$out/engine-metrics-$1.txt"
  grep -q '^matches_created_total{' "$out/engine-metrics-$1.txt" && grep -q '^match_window_seconds_count' "$out/engine-metrics-$1.txt"
}

# 0 -> >0 -> 0 réplicas en el CSV tras sembrar
scaled_up_and_down() {
  awk -F, 'NR > 1 && $2 > 0 { up = 1 } NR > 1 { last = $2 } END { exit !(up && last == 0) }' "$csv"
}

wait_scaled_cycle() {
  t0=$(date +%s)
  until scaled_up_and_down; do
    [ $(($(date +%s) - t0)) -lt 300 ] || return 1
    sleep 3
  done
  echo "ciclo completo $(($(date +%s) - t0)) s después de que terminó la siembra"
  awk -F, 'NR > 1 { if ($2 + 0 > max) max = $2 + 0 } END { print "pico de réplicas: " max }' "$csv"
}

# --- reglas: recarga en caliente -----------------------------------------------------------

# set_rules <archivo>: reemplaza rules.yaml en el ConfigMap matching-rules de r1
set_rules() {
  [ -s "$1" ] || return 1
  patch=$(jq -n --rawfile r "$1" '{data: {"rules.yaml": $r}}')
  k -n voltera-r1 patch configmap matching-rules --type merge -p "$patch"
}

rules_state() { curl -fsS "http://127.0.0.1:$PF_ENGINE_R1/debug/state" | jq -c '.rules'; }

# rules_is <versión|!versión> <error: yes|no>
rules_is() {
  s=$(rules_state) || return 1
  printf '%s\n' "$s"
  ver=$(printf '%s\n' "$s" | jq -r .version)
  err=$(printf '%s\n' "$s" | jq -r '.lastReloadError // ""')
  case $1 in
    !*) [ "$ver" != "${1#!}" ] || return 1 ;;
    *) [ "$ver" = "$1" ] || return 1 ;;
  esac
  if [ "$2" = yes ]; then [ -n "$err" ]; else [ -z "$err" ]; fi
}

rules_invalid_keeps_previous() {
  printf 'params: {}\neligible: "position.kwh >>"\npair: "true"\n' >"$out/rules-invalid.yaml"
  set_rules "$out/rules-invalid.yaml"
  retry 240 rules_is "$rules_v0" yes
}

rules_valid_switches() {
  sed 's/minKwh: 0.1/minKwh: 0.2/' "$rules_orig" >"$out/rules-v2.yaml"
  set_rules "$out/rules-v2.yaml"
  retry 240 rules_is "!$rules_v0" no
}

rules_restore() {
  set_rules "$rules_orig"
  retry 240 rules_is "$rules_v0" no
}

# --- reenvío de un EnergyMatched ya procesado ----------------------------------------------

resend_to_kafka() { # <region> <matchId>
  rs_payload=$(psql_r "$1" "SELECT payload::text FROM outbox WHERE key = '$2'")
  [ -n "$rs_payload" ] || return 1
  k -n "$NS" delete job s15-resend --ignore-not-found --wait=true
  k -n "$NS" delete configmap s15-resend --ignore-not-found
  k -n "$NS" create configmap s15-resend --from-literal=key="$2" --from-literal="value=$rs_payload" --from-literal="topic=$1.energy-matched"
  k apply -f "$HERE/resend-job.yaml"
  k -n "$NS" wait --for=condition=complete job/s15-resend --timeout=180s \
    || { k -n "$NS" logs job/s15-resend -c producer --tail=30; return 1; }
}

transfer_saw_duplicate() { # <ns> <matchId>
  k -n "$1" logs -l app=energy-transfer -c energy-transfer --tail=-1 --max-log-requests=10 2>/dev/null \
    | grep "\"matchId\":\"$2\"" | grep -q '"result":"duplicate"'
}

device_unchanged_for() { # <port> <matchId> <distinct0> <total0>
  state "$1" >"$out/device-after-resend.json" || return 1
  jq -e --arg id "$2" --argjson d "$3" --argjson t "$4" '.byMatchId[$id] == 1 and .distinct == $d and .total == $t' "$out/device-after-resend.json"
}

billing_saw_duplicate() { # <matchId> <distinct0> <duplicates0>
  curl -fsS "http://127.0.0.1:$PF_BILLING/state?detail=1" >"$out/billing-after-resend.json" || return 1
  jq -e --arg id "$1" --argjson d "$2" --argjson u "$3" '.byMatchId[$id].count == 2 and .distinct == $d and .duplicates == $u + 1' "$out/billing-after-resend.json"
}

transfer_single() { # <region> <matchId>
  c=$(psql_transfer "$1" "SELECT count(*) FROM transfers WHERE match_id = '$2' AND status = 'SENT'")
  echo "transferencias SENT del matchId: $c"
  [ "$c" = 1 ]
}

device_repost_is_duplicate() { # <port> <matchId> <key>
  body=$(printf '{"matchId":"%s","seller":"s","buyer":"b","kwh":1,"issuedAt":"%s"}' "$2" "$(date -u +%Y-%m-%dT%H:%M:%SZ)")
  sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$3" | awk '{print $NF}')
  resp=$(curl -fsS -X POST -H "X-Signature: $sig" -d "$body" "http://127.0.0.1:$1/commands")
  printf '%s\n' "$resp"
  printf '%s\n' "$resp" | jq -e '.status == "duplicate"' || return 1
  bad=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "X-Signature: deadbeef" -d "$body" "http://127.0.0.1:$1/commands")
  echo "firma inválida -> HTTP $bad"
  [ "$bad" = 401 ] || return 1
  state "$1" | jq -e --arg id "$2" '.byMatchId[$id] == 2 and .rejected >= 1 and .duplicates >= 1'
}

# ===========================================================================================

log "S15: despliegue (build de imágenes + apply de r1, r2 y billing)"
check "mise run apps:match-side all (imágenes, overlays, rollouts)" deploy_apps
check "port-forwards a device-sim r1/r2, billing-sim y matching-engine (puertos 19510-19514)" start_forwards
check "energy-transfer r1 en 0 réplicas (KEDA sin backlog)" retry 300 idle_at_zero

device_before=$(state "$PF_DEVICE_R1" || echo '{}')
printf '%s\n' "$device_before" >"$out/device-r1-before.json"
sample_loop &
pids="$pids $!"
sleep 4

log "S15: r1 con $N matches (prefijo $pfx_r1)"
t_seed_r1=$(date +%s)
check "siembra de la vista de r1: $N oferentes + $N demandantes en $zone_r1 y precio" seed r1 "$N" "$pfx_r1" "$zone_r1"
check "(a) r1: $N matches en matches-db" retry 60 matches_created r1 "$N" "$pfx_r1"
check "(a) r1: $((N * 2)) posiciones marcadas en matched_positions (cada una se empareja una vez)" marks_ok r1 "$N" "$pfx_r1"
check "(a) r1: outbox pendiente 0 (el relay publicó todo a Kafka)" retry 60 outbox_pending_zero r1
dump_ids r1 "$pfx_r1" "$out/ids-r1.json"
check "(b) device-sim r1: $N matchId con efecto, cada uno una vez (distinct == $N)" retry 150 device_has_all "$PF_DEVICE_R1" "$out/ids-r1.json" "$N"
check "(b) transfer-db r1: $N transferencias SENT" retry 60 transfers_sent r1 "$N" "$pfx_r1"
check "(c) billing-sim: $N matchId de r1, una vez cada uno" retry 90 billing_has_all "$out/ids-r1.json" r1

log "S15: r2 con $M matches (prefijo $pfx_r2)"
check "siembra de la vista de r2: $M oferentes + $M demandantes en $zone_r2 y precio" seed r2 "$M" "$pfx_r2" "$zone_r2"
check "(d) r2: $M matches en matches-db" retry 60 matches_created r2 "$M" "$pfx_r2"
check "(d) r2: outbox pendiente 0" retry 60 outbox_pending_zero r2
dump_ids r2 "$pfx_r2" "$out/ids-r2.json"
check "(d) device-sim r2: $M matchId con efecto, cada uno una vez" retry 150 device_has_all "$PF_DEVICE_R2" "$out/ids-r2.json" "$M"
check "(d) billing-sim: $M matchId de r2 atribuidos a r2" retry 90 billing_has_all "$out/ids-r2.json" r2
check "(d) billing-sim: r1 y r2 contados por separado (byRegion)" billing_regions_split "$N" "$M"
check "matching-engine expone matches_created_total y match_window_seconds" engine_metrics "$PF_ENGINE_R1"

check "(f) energy-transfer r1 escaló desde 0 por KEDA y volvió a 0 (hasta 5 min)" wait_scaled_cycle

log "S15: reglas (recarga en caliente) mientras vence la ventana de dedupe del stream"
rules_v0=$(rules_state | jq -r .version)
check "reglas iniciales cargadas (versión $rules_v0)" rules_is "$rules_v0" no
check "regla que no compila: se conserva la anterior y se registra el error (kubelet sincroniza el ConfigMap, hasta 4 min)" rules_invalid_keeps_previous
check "regla válida nueva: recarga atómica (cambia la versión, sin error)" rules_valid_switches
check "reglas originales restauradas (misma versión que al inicio)" rules_restore

wait_dedupe() {
  left=$((t_seed_r1 + DEDUP_WAIT - $(date +%s)))
  if [ "$left" -gt 0 ]; then
    log "esperando $left s: el stream descarta repetidos de Nats-Msg-Id durante 120 s"
    sleep "$left"
  fi
}
wait_dedupe

id1=$(jq -r '.[0]' "$out/ids-r1.json")
dev=$(state "$PF_DEVICE_R1" || echo '{}')
d_distinct=$(printf '%s\n' "$dev" | jq .distinct)
d_total=$(printf '%s\n' "$dev" | jq .total)
bil=$(state "$PF_BILLING" || echo '{}')
b_distinct=$(printf '%s\n' "$bil" | jq .distinct)
b_dups=$(printf '%s\n' "$bil" | jq .duplicates)
printf '%s\n' "$dev" >"$out/device-r1-before-resend.json"
printf '%s\n' "$bil" >"$out/billing-before-resend.json"
key=$(k -n voltera-r1 get secret device-command-key -o jsonpath='{.data.DEVICE_COMMAND_KEY}' | base64 -d)

log "S15: (e) reenvío de $id1 a Kafka"
check "namespace $NS" ensure_ns "$NS"
check "(e) reenvío del EnergyMatched ya procesado a r1.energy-matched (Job con kafka-console-producer)" resend_to_kafka r1 "$id1"
check "(e) energy-transfer reescala desde 0 y registra el duplicado (inbox)" retry 150 transfer_saw_duplicate voltera-r1 "$id1"
check "(e) device-sim r1: sin comando nuevo (byMatchId[$id1] == 1; distinct y total sin cambio)" device_unchanged_for "$PF_DEVICE_R1" "$id1" "$d_distinct" "$d_total"
check "(e) transfer-db: una sola transferencia SENT para el matchId" transfer_single r1 "$id1"
check "(e) billing-sim: mismo distinct, duplicates +1 (el stream aceptó el repetido fuera de la ventana de dedupe)" retry 90 billing_saw_duplicate "$id1" "$b_distinct" "$b_dups"
check "(e) device-sim: un comando firmado repetido responde 200 duplicate sin nuevo efecto; firma inválida 401" device_repost_is_duplicate "$PF_DEVICE_R1" "$id1" "$key"
check "(f) segundo ciclo: energy-transfer vuelve a 0 tras el duplicado" retry 240 idle_at_zero

# --- evidencia -----------------------------------------------------------------------------
state "$PF_DEVICE_R1" >"$out/device-r1-final.json" || true
state "$PF_DEVICE_R2" >"$out/device-r2-final.json" || true
curl -fsS "http://127.0.0.1:$PF_BILLING/state" >"$out/billing-final.json" || true
for r in r1 r2; do
  k -n "voltera-$r" logs deploy/matching-engine -c matching-engine --tail=200 >"$out/matching-engine-$r.log" 2>&1 || true
  k -n "voltera-$r" logs deploy/mesh-bridge -c mesh-bridge --tail=200 >"$out/mesh-bridge-$r.log" 2>&1 || true
  k -n "voltera-$r" logs deploy/device-sim -c device-sim --tail=200 >"$out/device-sim-$r.log" 2>&1 || true
  k -n "voltera-$r" get scaledobject,hpa >"$out/keda-$r.txt" 2>&1 || true
done
k -n voltera-global logs deploy/billing-sim -c billing-sim --tail=200 >"$out/billing-sim.log" 2>&1 || true
k top pods -A --containers 2>&1 | grep -E 'NAME|matching-engine|energy-transfer|device-sim|mesh-bridge|billing-sim' >"$out/top-pods.txt" || true
cat "$out/top-pods.txt"
log "muestras de réplicas en ${csv#"$ROOT"/}"
finish "$NS"
