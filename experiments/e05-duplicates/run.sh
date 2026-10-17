#!/bin/sh
# E5: idempotencia por duplicados (gateway, energy-transfer, billing-sim) en r1. Se carga con `.` desde experiments/run.sh.
# Define drive y collect; usa OUT, EXPCTL, expctl, k, log y chaos_* del runner.
# shellcheck disable=SC2154  # variables del runner (OUT, ROOT, EXP_ID)
# shellcheck source=experiments/lib/metersim.sh
. "$ROOT/experiments/lib/metersim.sh"
# shellcheck source=experiments/lib/e56.sh
. "$ROOT/experiments/lib/e56.sh"

NS=voltera-r1
TESTNS=e05-duplicates
GW=declaration-gateway
TR=energy-transfer
PF_DEVICE=19901
PF_BILLING=19902

E5_METERS=${E5_METERS:-300}          # (a) medidores únicos con v5
E5_K=${E5_K:-6}                      # (a) copias de cada v5
E5_METERS_V3=${E5_METERS_V3:-150}    # (a) medidores que reciben después una v3 (vieja)
E5_K3=${E5_K3:-4}                    # (a) copias de cada v3
E5_GW_REPLICAS=${E5_GW_REPLICAS:-3}
E5_TR_REPLICAS=${E5_TR_REPLICAS:-2}
E5_PAIRS_RATE=${E5_PAIRS_RATE:-20}   # (b) declaraciones/s del lote que genera los matches a reenviar
E5_PAIRS_S=${E5_PAIRS_S:-10}
E5_RESEND_K=${E5_RESEND_K:-5}        # (b) copias de cada EnergyMatched reenviado
E5_DEDUP_WAIT=${E5_DEDUP_WAIT:-135}  # > duplicate_window del stream (120 s)
E5_C_RATE=${E5_C_RATE:-20}           # (c) declaraciones/s bajo crashes
E5_C_S=${E5_C_S:-30}
E5_C_GW_N=${E5_C_GW_N:-60}           # (c) before_ack=crash-after:N en el gateway
E5_C_TR_N=${E5_C_TR_N:-40}           # (c) before_ack=crash-after:N en energy-transfer
E5_C_MAX=${E5_C_MAX:-3}              # (c) crashes máximos por servicio

RUN=$(date -u +%d%H%M%S)             # prefijos únicos por corrida: no se mezcla con tráfico previo
PFX_A=e05-$RUN-a-
PFX_B=e05-$RUN-b-
PFX_C=e05-$RUN-c-

dev_state() { curl -fsS --max-time 10 "http://127.0.0.1:$PF_DEVICE/state"; }
bill_state() { curl -fsS --max-time 10 "http://127.0.0.1:$PF_BILLING/state?detail=1"; }
psql_pos() { e56_psql "$NS" positions-db "$1"; }
psql_m() { e56_psql "$NS" matches-db "$1"; }
psql_tr() { e56_psql "$NS" transfer-db "$1"; }

# in_prefix <prefix>: condición SQL de matches/transfers donde interviene un medidor del prefijo
# in_prefix <prefix> [alias.]
in_prefix() { printf "(%sseller_meter_id LIKE '%s%%' OR %sbuyer_meter_id LIKE '%s%%')" "${2:-}" "$1" "${2:-}" "$1"; }

# cleanup: failpoints a off, KEDA como estaba, sin procesos de fondo, sin recursos de prueba
e05_cleanup() {
  e56_stop_bg
  e56_disarm "$NS" "$GW" before_ack
  e56_disarm "$NS" "$TR" before_ack
  e56_keda_restore
  k delete namespace "$TESTNS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  cluster_unlock
}
trap e05_cleanup EXIT
trap 'exit 130' INT TERM

exp_reset() {
  mkdir -p "$OUT/raw"
  echo '{}' >"$OUT/raw/m.json"
  e56_pf voltera-r1 svc/device-sim "$PF_DEVICE" 8080
  e56_pf voltera-global svc/billing-sim "$PF_BILLING" 8080
  e56_wait 60 dev_state >/dev/null || return 1
  e56_wait 60 bill_state >/dev/null || return 1
  e56_ensure_ns "$TESTNS" || return 1
  # KEDA fija las réplicas: los contadores por pod sólo se pueden sumar si no entran ni salen pods
  e56_keda_pause "$NS" "$GW" "$E5_GW_REPLICAS" || return 1
  e56_keda_pause "$NS" "$TR" "$E5_TR_REPLICAS" || return 1
  msets prefixA "$PFX_A"
  msets prefixB "$PFX_B"
  msets prefixC "$PFX_C"
}

# run_load <archivo> <args de meter-sim...>: corre meter-sim y deja stdout completo; el resumen JSON va a <archivo>.json
run_load() {
  _f=$1
  shift
  metersim_run r1 "$@" --out /dev/stdout >"$_f" 2>&1 || { log "meter-sim falló: $_f"; return 1; }
  metersim_summary <"$_f" >"$_f.json"
  grep -E '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12},' "$_f" >"$_f.csv" || true
}

# --- esperas -------------------------------------------------------------------------------

matches_stable() { # <prefix> <archivo con el último conteo>
  _c=$(psql_m "SELECT count(*) FROM matches WHERE $(in_prefix "$1")")
  _p=$(cat "$2" 2>/dev/null || echo -1)
  echo "$_c" >"$2"
  [ "$_c" -gt 0 ] && [ "$_c" = "$_p" ]
}

# device_all_once <idsfile>: cada matchId tiene exactamente 1 efecto en device-sim
device_all_once() {
  dev_state >"$OUT/raw/device-poll.json" || return 1
  jq -e --rawfile ids "$1" '
    ($ids | split("\n") | map(select(length > 0))) as $l
    | ($l | length) > 0 and ([$l[] as $i | .byMatchId[$i]] | all(. == 1))' "$OUT/raw/device-poll.json" >/dev/null
}

transfers_all_sent() { # <prefix> <n>
  _c=$(psql_tr "SELECT count(*) FROM transfers WHERE status = 'SENT' AND $(in_prefix "$1")")
  [ "$_c" = "$2" ]
}

# settle_matches <prefix> <nombre>: espera a que los matches del prefijo estén creados, publicados y aplicados; deja sus ids
settle_matches() {
  e56_wait 120 matches_stable "$1" "$OUT/raw/$2.count" || return 1
  e56_wait 60 matches_stable "$1" "$OUT/raw/$2.count" || return 1
  psql_m "SELECT match_id FROM matches WHERE $(in_prefix "$1") ORDER BY 1" >"$OUT/raw/$2-ids.txt"
  _n=$(wc -l <"$OUT/raw/$2-ids.txt" | tr -d ' ')
  e56_wait 120 transfers_all_sent "$1" "$_n" || return 1
  e56_wait 60 device_all_once "$OUT/raw/$2-ids.txt"
}

outbox_pending_zero() { [ "$(psql_pos "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ]; }

# --- (a) gateway: k copias y desorden v5 -> v3 --------------------------------------------

gw_consumed() { e56_metric_sum "$NS" "$GW" events_consumed_total 'channel="meter-declarations"' "result=\"$1\""; }

gw_processed_is() { # <esperado>
  _t=$(($(gw_consumed ok) + $(gw_consumed stale) + $(gw_consumed error)))
  [ "$_t" = "$1" ]
}

redis_versions() { # <prefix>: "clave versión" por cada key pos:* del prefijo
  k -n "$NS" exec deploy/market-view -c redis -- redis-cli --scan --pattern "pos:*:$1*" >"$OUT/raw/redis-keys.txt"
  [ -s "$OUT/raw/redis-keys.txt" ] || return 0
  sed 's/^/HGET /; s/$/ _v/' "$OUT/raw/redis-keys.txt" \
    | k -n "$NS" exec -i deploy/market-view -c redis -- redis-cli >"$OUT/raw/redis-vals.txt"
  paste -d' ' "$OUT/raw/redis-keys.txt" "$OUT/raw/redis-vals.txt"
}

phase_a() {
  log "E5 (a): gateway, $E5_METERS medidores x $E5_K copias de v5, luego $E5_METERS_V3 x $E5_K3 copias de v3"
  _ok0=$(gw_consumed ok)
  _st0=$(gw_consumed stale)
  _er0=$(gw_consumed error)
  _dd0=$(e56_metric_sum "$NS" "$GW" events_duplicates_dropped_total 'event_type="PositionDeclared"')
  _t0=$(($(gw_consumed ok) + $(gw_consumed stale) + $(gw_consumed error)))
  run_load "$OUT/raw/a-v5.log" --profile dup --meters "$E5_METERS" --dup-k "$E5_K" --version-base 5 \
    --prefix "$PFX_A" --seed 5 --rate 300 || return 1
  run_load "$OUT/raw/a-v3.log" --profile dup --meters "$E5_METERS_V3" --dup-k "$E5_K3" --version-base 3 \
    --prefix "$PFX_A" --seed 5 --rate 300 || return 1
  _pub5=$(jq .published "$OUT/raw/a-v5.log.json")
  _pub3=$(jq .published "$OUT/raw/a-v3.log.json")
  _deliveries=$((_pub5 + _pub3))
  mset a_published_errors "$(($(jq .errors "$OUT/raw/a-v5.log.json") + $(jq .errors "$OUT/raw/a-v3.log.json") + $(jq .dropped "$OUT/raw/a-v5.log.json") + $(jq .dropped "$OUT/raw/a-v3.log.json")))"
  e56_wait 180 gw_processed_is $((_t0 + _deliveries)) || log "el gateway no llegó a procesar $_deliveries entregas"
  sleep 3
  _ok=$(($(gw_consumed ok) - _ok0))
  _st=$(($(gw_consumed stale) - _st0))
  _er=$(($(gw_consumed error) - _er0))
  mset a_deliveries "$_deliveries"
  mset a_applied "$_ok"
  mset a_stale "$_st"
  mset a_errors "$_er"
  mset a_duplicates_dropped "$(($(e56_metric_sum "$NS" "$GW" events_duplicates_dropped_total 'event_type="PositionDeclared"') - _dd0))"
  # balance: aplicados + descartados (stale incluye los duplicados exactos: misma versión) - entregas
  mset a_balance_diff "$((_ok + _st + _er - _deliveries))"
  mset a_meters "$E5_METERS"

  e56_wait 60 outbox_pending_zero || true
  mset a_outbox_rows "$(psql_pos "SELECT count(*) FROM outbox WHERE aggregate_id LIKE '$PFX_A%'")"
  mset a_outbox_dup_groups "$(psql_pos "SELECT count(*) FROM (SELECT aggregate_id, payload->>'version' FROM outbox WHERE aggregate_id LIKE '$PFX_A%' GROUP BY 1, 2 HAVING count(*) > 1) g")"
  mset a_outbox_not_v5 "$(psql_pos "SELECT count(*) FROM outbox WHERE aggregate_id LIKE '$PFX_A%' AND payload->>'version' <> '5'")"
  mset a_positions_v5 "$(psql_pos "SELECT count(*) FROM positions WHERE meter_id LIKE '$PFX_A%' AND version = 5")"
  psql_pos "SELECT event_id::text FROM outbox WHERE aggregate_id LIKE '$PFX_A%'" >"$OUT/raw/a-effects.txt"

  # Redis: la vista conserva v5 (el proyector nunca vio la v3; la TTL de 150 s no debe haber vencido)
  redis_versions "$PFX_A" >"$OUT/raw/a-redis.txt" || true
  mset a_redis_checked "$(wc -l <"$OUT/raw/a-redis.txt" | tr -d ' ')"
  mset a_redis_not_v5 "$(awk '$2 != 5 { n++ } END { print n + 0 }' "$OUT/raw/a-redis.txt")"
}

# --- (b) energy-transfer: reenvío de EnergyMatched ya procesados --------------------------

phase_b0() {
  log "E5 (b): genera matches con carga pairs ($E5_PAIRS_RATE decl/s x ${E5_PAIRS_S}s) para reenviarlos después"
  run_load "$OUT/raw/b-pairs.log" --profile pairs --meters $((E5_PAIRS_RATE * E5_PAIRS_S)) --rate "$E5_PAIRS_RATE" \
    --duration "${E5_PAIRS_S}s" --prefix "$PFX_B" --seed 6 || return 1
  settle_matches "$PFX_B" b || return 1
  B_DONE=$(date +%s)
  _n=$(wc -l <"$OUT/raw/b-ids.txt" | tr -d ' ')
  log "E5 (b): $_n matches procesados"
  mset b_matches "$_n"
}

tr_dups_ge() { # <base> <mínimo>: duplicados descartados por energy-transfer desde <base>
  [ $(($(e56_metric_sum "$NS" "$TR" events_duplicates_dropped_total 'event_type="EnergyMatched"') - $1)) -ge "$2" ]
}

# billing_two: cada matchId de b-ids.txt con exactamente 2 entregas en billing-sim
billing_two() {
  bill_state >"$OUT/raw/billing-after.json" || return 1
  jq -e --rawfile ids "$OUT/raw/b-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | .byMatchId[$i].count] | all(. == 2)' \
    "$OUT/raw/billing-after.json" >/dev/null
}

phase_b() {
  _n=$(wc -l <"$OUT/raw/b-ids.txt" | tr -d ' ')
  _left=$((B_DONE + E5_DEDUP_WAIT - $(date +%s)))
  if [ "$_left" -gt 0 ]; then
    log "E5 (b/d): esperando $_left s para que venza la duplicate_window de 120 s del stream"
    sleep "$_left"
  fi
  dev_state >"$OUT/raw/device-before.json" || return 1
  bill_state >"$OUT/raw/billing-before.json" || return 1
  _dup0=$(e56_metric_sum "$NS" "$TR" events_duplicates_dropped_total 'event_type="EnergyMatched"')
  _cdup0=$(e56_metric_sum "$NS" "$TR" events_consumed_total 'event_type="EnergyMatched"' 'result="duplicate"')
  _cok0=$(e56_metric_sum "$NS" "$TR" events_consumed_total 'event_type="EnergyMatched"' 'result="ok"')
  _bdup0=$(jq '.byRegion.r1.duplicates // 0' "$OUT/raw/billing-before.json")

  # sobres tal cual los guardó la outbox (mismo eventId, misma clave matchId), K veces cada uno
  psql_m "SELECT key || '|' || payload::text FROM outbox WHERE key IN (SELECT match_id FROM matches WHERE $(in_prefix "$PFX_B")) ORDER BY id" \
    >"$OUT/raw/b-envelopes.txt"
  [ "$(wc -l <"$OUT/raw/b-envelopes.txt" | tr -d ' ')" = "$_n" ] || { log "faltan sobres en la outbox"; return 1; }
  : >"$OUT/raw/b-resend.txt"
  _i=0
  while [ "$_i" -lt "$E5_RESEND_K" ]; do
    cat "$OUT/raw/b-envelopes.txt" >>"$OUT/raw/b-resend.txt"
    _i=$((_i + 1))
  done
  k -n "$TESTNS" delete configmap e05-resend --ignore-not-found >/dev/null
  k -n "$TESTNS" create configmap e05-resend --from-file=msgs.txt="$OUT/raw/b-resend.txt" >/dev/null || return 1
  kafka_job "$TESTNS" e05-resend e05-resend \
    '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka.voltera-infra.svc:9092 --topic r1.energy-matched --property parse.key=true --property "key.separator=|" </in/msgs.txt' \
    >"$OUT/raw/b-resend-job.log" || { log "el Job de reenvío falló"; return 1; }
  _want=$((_n * E5_RESEND_K))
  log "E5 (b): $_want entregas reenviadas ($_n matches x $E5_RESEND_K)"

  e56_wait 180 tr_dups_ge "$_dup0" "$_want" || log "energy-transfer no llegó a $_want duplicados"
  sleep 3
  mset b_resent "$_want"
  mset b_dup_dropped_delta "$(($(e56_metric_sum "$NS" "$TR" events_duplicates_dropped_total 'event_type="EnergyMatched"') - _dup0))"
  mset b_consumed_dup_delta "$(($(e56_metric_sum "$NS" "$TR" events_consumed_total 'event_type="EnergyMatched"' 'result="duplicate"') - _cdup0))"
  mset b_consumed_ok_delta "$(($(e56_metric_sum "$NS" "$TR" events_consumed_total 'event_type="EnergyMatched"' 'result="ok"') - _cok0))"

  # efectos: device-sim, transfers e inbox
  dev_state >"$OUT/raw/device-after.json" || return 1
  mset b_device_not_once "$(jq --rawfile ids "$OUT/raw/b-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | .byMatchId[$i]] | map(select(. != 1)) | length' "$OUT/raw/device-after.json")"
  mset b_device_dups_delta "$(( $(jq .duplicates "$OUT/raw/device-after.json") - $(jq .duplicates "$OUT/raw/device-before.json") ))"
  mset b_transfers_rows "$(psql_tr "SELECT count(*) FROM transfers WHERE $(in_prefix "$PFX_B")")"
  mset b_transfers_sent "$(psql_tr "SELECT count(*) FROM transfers WHERE status = 'SENT' AND $(in_prefix "$PFX_B")")"
  mset b_transfers_dup_groups "$(psql_tr "SELECT count(*) FROM (SELECT match_id FROM transfers WHERE $(in_prefix "$PFX_B") GROUP BY 1 HAVING count(*) > 1) g")"
  mset b_inbox_rows "$(psql_tr "SELECT count(*) FROM inbox i JOIN transfers t ON t.match_id = i.event_key WHERE i.consumer = 'energy-transfer' AND $(in_prefix "$PFX_B" t.)")"

  # (d) billing-sim: el stream aceptó 1 copia por matchId (las demás cayeron en su duplicate_window)
  e56_wait 120 billing_two || log "billing-sim no llegó a 2 entregas por matchId"
  sleep 5
  bill_state >"$OUT/raw/billing-after.json" || return 1
  mset d_ids_with_two "$(jq --rawfile ids "$OUT/raw/b-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | .byMatchId[$i].count] | map(select(. == 2)) | length' "$OUT/raw/billing-after.json")"
  mset d_ids_other "$(jq --rawfile ids "$OUT/raw/b-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | .byMatchId[$i].count] | map(select(. != 2)) | length' "$OUT/raw/billing-after.json")"
  mset d_duplicates_delta "$(( $(jq '.byRegion.r1.duplicates // 0' "$OUT/raw/billing-after.json") - _bdup0 ))"
  # distinct de MIS matchIds: no cambia con los reenvíos
  mset d_distinct_mine_before "$(jq --rawfile ids "$OUT/raw/b-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | select(.byMatchId[$i] != null)] | length' "$OUT/raw/billing-before.json")"
  mset d_distinct_mine_after "$(jq --rawfile ids "$OUT/raw/b-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | select(.byMatchId[$i] != null)] | length' "$OUT/raw/billing-after.json")"
}

# --- (c) redelivery real: before_ack con crash bajo carga pairs ---------------------------

# pod_logs <deploy>: logs del contenedor actual y del anterior de todos los pods
pod_logs() {
  _sel=$(_chaos_selector "$NS" "$1") || return 0
  for _pod in $(_chaos_pods "$NS" "$_sel"); do
    k -n "$NS" logs "$_pod" -c "$1" --tail=-1 2>/dev/null || true
    k -n "$NS" logs "$_pod" -c "$1" --previous --tail=-1 2>/dev/null || true
  done
}

phase_c() {
  log "E5 (c): before_ack=crash-after en $GW (N=$E5_C_GW_N) y $TR (N=$E5_C_TR_N) bajo carga pairs"
  e56_bg e56_rearm_loop "$NS" "$GW" before_ack "crash-after:$E5_C_GW_N" "$E5_C_MAX" "$OUT/raw/crashes-gateway.csv"
  _pid_gw=$E56_LAST
  e56_bg e56_rearm_loop "$NS" "$TR" before_ack "crash-after:$E5_C_TR_N" "$E5_C_MAX" "$OUT/raw/crashes-transfer.csv"
  _pid_tr=$E56_LAST
  sleep 3
  run_load "$OUT/raw/c-pairs.log" --profile pairs --meters $((E5_C_RATE * E5_C_S)) --rate "$E5_C_RATE" \
    --duration "${E5_C_S}s" --prefix "$PFX_C" --seed 7 || return 1
  settle_matches "$PFX_C" c || return 1
  e56_wait 60 outbox_pending_zero || true
  e56_kill "$_pid_gw"
  e56_kill "$_pid_tr"
  e56_disarm "$NS" "$GW" before_ack
  e56_disarm "$NS" "$TR" before_ack

  _pub=$(jq .published "$OUT/raw/c-pairs.log.json")
  mset c_published "$_pub"
  mset c_lost_in_sender "$(($(jq .errors "$OUT/raw/c-pairs.log.json") + $(jq .dropped "$OUT/raw/c-pairs.log.json")))"
  mset c_gw_crashes "$(awk -F, 'NR > 1 { s += $5 } END { print s + 0 }' "$OUT/raw/crashes-gateway.csv")"
  mset c_tr_crashes "$(awk -F, 'NR > 1 { s += $5 } END { print s + 0 }' "$OUT/raw/crashes-transfer.csv")"
  # redelivery observada en los logs (contenedor actual y anterior de cada pod): el gateway ve como stale una declaración
  # del prefijo (cada (medidor, versión) se envió una vez, luego es una reentrega); energy-transfer registra duplicados de inbox
  mset c_gw_redelivered "$(pod_logs "$GW" | grep -F "\"meterId\":\"$PFX_C" | grep -c '"result":"stale"' || true)"
  mset c_tr_redelivered "$(pod_logs "$TR" | grep -c '"stage":"apply","result":"duplicate"' || true)"
  psql_pos "SELECT event_id::text FROM outbox WHERE aggregate_id LIKE '$PFX_C%'" >"$OUT/raw/c-effects.txt"
  mset c_outbox_rows "$(psql_pos "SELECT count(*) FROM outbox WHERE aggregate_id LIKE '$PFX_C%'")"
  mset c_outbox_dup_groups "$(psql_pos "SELECT count(*) FROM (SELECT aggregate_id, payload->>'version' FROM outbox WHERE aggregate_id LIKE '$PFX_C%' GROUP BY 1, 2 HAVING count(*) > 1) g")"
  mset c_outbox_pending "$(psql_pos "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
  mset c_matches "$(wc -l <"$OUT/raw/c-ids.txt" | tr -d ' ')"
  dev_state >"$OUT/raw/device-c.json" || return 1
  mset c_device_not_once "$(jq --rawfile ids "$OUT/raw/c-ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | .byMatchId[$i]] | map(select(. != 1)) | length' "$OUT/raw/device-c.json")"
  mset c_transfers_sent "$(psql_tr "SELECT count(*) FROM transfers WHERE status = 'SENT' AND $(in_prefix "$PFX_C")")"
  mset c_transfers_not_sent "$(psql_tr "SELECT count(*) FROM transfers WHERE status <> 'SENT' AND $(in_prefix "$PFX_C")")"
  mset c_transfers_dup_groups "$(psql_tr "SELECT count(*) FROM (SELECT match_id FROM transfers WHERE $(in_prefix "$PFX_C") GROUP BY 1 HAVING count(*) > 1) g")"
}

drive() {
  phase_b0 || return 1
  phase_a || return 1
  phase_c || return 1
  phase_b || return 1
}

# --- collect -------------------------------------------------------------------------------

# reconciliation.csv: por eventId (gateway) y por matchId (transfer/billing), cuántas veces se entregó y cuántos efectos hubo
collect() {
  _t0=$1 _t1=$2 _dir=$3
  _rec="$_dir/reconciliation.csv"
  echo "channel,id,delivered,effects,expected_effects" >"$_rec"
  # gateway: entregas por eventId (CSV de meter-sim) vs filas de outbox con ese event_id; la v3 es vieja: efecto esperado 0
  for _run in a-v5 a-v3 c-pairs; do
    case $_run in
      a-*) _eff=$_dir/raw/a-effects.txt ;;
      *) _eff=$_dir/raw/c-effects.txt ;;
    esac
    case $_run in
      a-v3) _want=0 ;;
      *) _want=1 ;;
    esac
    awk -F, -v ch="gateway:$_run" -v want="$_want" -v eff="$_eff" '
      FILENAME == eff { e[$1]++; next }
      { d[$1]++ }
      END { for (id in d) print ch "," id "," d[id] "," (e[id] + 0) "," want }' "$_eff" "$_dir/raw/$_run.sent" | sort >>"$_rec"
  done
  # transfer y billing (b): el original + K reenvíos; device-sim aplica 1 comando y billing-sim registra 2 entregas (original + 1 aceptada por el stream)
  jq -r --rawfile ids "$_dir/raw/b-ids.txt" --argjson k "$E5_RESEND_K" \
    '($ids | split("\n") | map(select(length > 0)))[] as $i | "transfer:b,\($i),\($k + 1),\(.byMatchId[$i] // 0),1"' \
    "$_dir/raw/device-after.json" | sort >>"$_rec"
  jq -r --rawfile ids "$_dir/raw/b-ids.txt" --argjson k "$E5_RESEND_K" \
    '($ids | split("\n") | map(select(length > 0)))[] as $i | "billing:b,\($i),\($k + 1),\(.byMatchId[$i].count // 0),2"' \
    "$_dir/raw/billing-after.json" | sort >>"$_rec"
  mset reconciliation_mismatches "$(awk -F, 'NR > 1 && $4 != $5 { n++ } END { print n + 0 }' "$_rec")"
  mset reconciliation_rows "$(($(wc -l <"$_rec") - 1))"

  jq -n --slurpfile m "$_dir/raw/m.json" '$m[0] | {device: {b_not_once: .b_device_not_once, c_not_once: .c_device_not_once, b_dups_delta: .b_device_dups_delta},
    transfers: {b_rows: .b_transfers_rows, b_sent: .b_transfers_sent, c_sent: .c_transfers_sent, c_not_sent: .c_transfers_not_sent},
    gateway: {a_outbox_rows: .a_outbox_rows, c_outbox_rows: .c_outbox_rows},
    billing: {ids_with_two: .d_ids_with_two, ids_other: .d_ids_other}}' >"$_dir/effects.json"
  jq -n --slurpfile m "$_dir/raw/m.json" '$m[0] | {gateway: {deliveries: .a_deliveries, applied: .a_applied, stale: .a_stale, errors: .a_errors, balance_diff: .a_balance_diff},
    energy_transfer: {resent: .b_resent, duplicates_dropped_delta: .b_dup_dropped_delta, consumed_duplicate_delta: .b_consumed_dup_delta},
    billing: {duplicates_delta: .d_duplicates_delta}}' >"$_dir/dup_counters.json"
  # las comparaciones con un conteo medido se expresan como diferencias (criteria.yaml compara contra constantes)
  jq --arg t0 "$_t0" --arg t1 "$_t1" '. + {tStart: $t0, tEnd: $t1,
    a_outbox_rows_diff: (.a_outbox_rows - .a_meters), a_positions_v5_diff: (.a_positions_v5 - .a_meters),
    a_applied_diff: (.a_applied - .a_meters), a_redis_diff: (.a_redis_checked - .a_meters),
    b_transfers_sent_diff: (.b_transfers_sent - .b_matches), b_inbox_diff: (.b_inbox_rows - .b_matches),
    b_dup_dropped_diff: (.b_dup_dropped_delta - .b_resent), d_two_diff: (.d_ids_with_two - .b_matches),
    d_distinct_diff: (.d_distinct_mine_after - .b_matches), d_dups_diff: (.d_duplicates_delta - .b_matches),
    c_outbox_rows_diff: (.c_outbox_rows - .c_published)}' "$_dir/raw/m.json" >"$_dir/metrics.json"
}
