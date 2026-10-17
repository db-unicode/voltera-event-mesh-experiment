#!/bin/sh
# E6: outbox transaccional. matching-engine muere entre el commit y la publicación; el control (OUTBOX_MODE=direct)
# hace escritura dual ingenua y debe perder eventos. Se carga con `.` desde experiments/run.sh.
# Define drive y collect; usa OUT, ROOT, expctl, k, log y chaos_* del runner.
# shellcheck disable=SC2154  # variables del runner (OUT, ROOT)
# shellcheck source=experiments/lib/metersim.sh
. "$ROOT/experiments/lib/metersim.sh"
# shellcheck source=experiments/lib/e56.sh
. "$ROOT/experiments/lib/e56.sh"

NS=voltera-r1
TESTNS=e06-outbox
ENG=matching-engine
TOPIC=r1.energy-matched
PF_DEVICE=19911

E6_RATE=${E6_RATE:-20}                  # pares/s (2 declaraciones por par)
E6_S=${E6_S:-120}                       # duración de las fases con outbox
E6_CONTROL_S=${E6_CONTROL_S:-60}        # duración del control
E6_CRASH_N=${E6_CRASH_N:-200}           # crash-after:N
E6_CRASHES=${E6_CRASHES:-2}             # crashes máximos por fase con outbox
E6_DRAIN_S=${E6_DRAIN_S:-180}           # espera máxima de drenaje tras la carga
E6_KAFKA_TRIES=${E6_KAFKA_TRIES:-3}     # lecturas del topic (se unen) hasta que no falte ningún match

RUN=$(date -u +%d%H%M%S)
MODE_ORIG=""
MODE_CHANGED=0

dev_state() { curl -fsS --max-time 10 "http://127.0.0.1:$PF_DEVICE/state"; }
psql_m() { e56_psql "$NS" matches-db "$1"; }
psql_tr() { e56_psql "$NS" transfer-db "$1"; }

# in_prefix <prefix> [alias.]: matches donde interviene un medidor del prefijo
in_prefix() { printf "(%sseller_meter_id LIKE '%s%%' OR %sbuyer_meter_id LIKE '%s%%')" "${2:-}" "$1" "${2:-}" "$1"; }

# set_mode <outbox|direct>: OUTBOX_MODE del matching-engine (sólo el control usa direct); espera el rollout
set_mode() {
  if [ "$1" = outbox ] && [ -z "$MODE_ORIG" ]; then
    k -n "$NS" set env "deploy/$ENG" OUTBOX_MODE- >/dev/null || return 1
  elif [ "$1" = outbox ]; then
    k -n "$NS" set env "deploy/$ENG" "OUTBOX_MODE=$MODE_ORIG" >/dev/null || return 1
  else
    k -n "$NS" set env "deploy/$ENG" "OUTBOX_MODE=$1" >/dev/null || return 1
  fi
  MODE_CHANGED=1
  wait_rollout "deploy/$ENG" "$NS" 180s || return 1
  wait_ready "$NS" "$ENG" 120
}

# engine_started_with <modo>: /debug/state declara el modo (una imagen vieja no trae el campo y el control no sería un control)
engine_started_with() {
  [ "$(expctl http-get "$NS/deploy/$ENG:9090/debug/state" | jq -r '.outboxMode // ""')" = "$1" ]
}

e06_cleanup() {
  e56_stop_bg
  e56_disarm "$NS" "$ENG" after_commit_before_publish after_publish_before_mark
  if [ "$MODE_CHANGED" = 1 ]; then
    if [ -z "$MODE_ORIG" ]; then
      k -n "$NS" set env "deploy/$ENG" OUTBOX_MODE- >/dev/null 2>&1 || true
    else
      k -n "$NS" set env "deploy/$ENG" "OUTBOX_MODE=$MODE_ORIG" >/dev/null 2>&1 || true
    fi
    k -n "$NS" rollout status "deploy/$ENG" --timeout=120s >/dev/null 2>&1 || true
  fi
  e56_keda_restore
  k delete namespace "$TESTNS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  cluster_unlock
}
trap e06_cleanup EXIT
trap 'exit 130' INT TERM

exp_reset() {
  mkdir -p "$OUT/raw"
  echo '{}' >"$OUT/raw/m.json"
  MODE_ORIG=$(k -n "$NS" get deploy "$ENG" -o json | jq -r '[.spec.template.spec.containers[0].env[]? | select(.name == "OUTBOX_MODE") | .value][0] // ""')
  e56_pf "$NS" svc/device-sim "$PF_DEVICE" 8080
  e56_wait 60 dev_state >/dev/null || return 1
  e56_ensure_ns "$TESTNS" || return 1
  msets outboxModeOriginal "${MODE_ORIG:-outbox (sin variable)}"
  # imagen con OUTBOX_MODE: si el pod actual la ignora (imagen anterior), se reinicia una vez para tomar la imagen cargada en kind
  if ! engine_started_with "${MODE_ORIG:-outbox}"; then
    log "$ENG sin outboxMode en /debug/state: reinicio para tomar la imagen actual"
    k -n "$NS" rollout restart "deploy/$ENG" >/dev/null && wait_rollout "deploy/$ENG" "$NS" 180s && wait_ready "$NS" "$ENG" 120 || return 1
    engine_started_with "${MODE_ORIG:-outbox}" || {
      log "/debug/state de $ENG sigue sin outboxMode: reconstruye la imagen (mise run images:build matching-engine)"
      return 1
    }
  fi
}

# --- helpers de fase -----------------------------------------------------------------------

sampler() { # <csv> <prefix>
  echo "ts,pending,matches,restarts" >"$1"
  while :; do
    _ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    _row=$(psql_m "SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL) || ',' || (SELECT count(*) FROM matches WHERE $(in_prefix "$2"))" 2>/dev/null) || _row=","
    _r=$(e56_pod_restarts "$NS" "$ENG" 2>/dev/null) || _r=""
    echo "$_ts,$_row,$_r" >>"$1"
    sleep 1
  done
}

pending_zero() { [ "$(psql_m "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ]; }

matches_stable() { # <prefix> <archivo>
  _c=$(psql_m "SELECT count(*) FROM matches WHERE $(in_prefix "$1")")
  _p=$(cat "$2" 2>/dev/null || echo -1)
  echo "$_c" >"$2"
  [ "$_c" -gt 0 ] && [ "$_c" = "$_p" ]
}

device_has_all() { # <idsfile>
  dev_state >"$OUT/raw/device-poll.json" || return 1
  jq -e --rawfile ids "$1" '
    ($ids | split("\n") | map(select(length > 0))) as $l
    | ($l | length) > 0 and ([$l[] as $i | .byMatchId[$i]] | all(. == 1))' "$OUT/raw/device-poll.json" >/dev/null
}

# kafka_read <prefix> <salida>: matchId,eventId,partition de cada registro del topic con ese prefijo (une varias lecturas)
kafka_read() {
  _pfx=$1 _dest=$2
  : >"$_dest"
  _script="/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka.voltera-infra.svc:9092 --topic $TOPIC --from-beginning --timeout-ms 20000 --property print.key=true 2>/dev/null"
  kafka_job "$TESTNS" e06-read - "$_script" >"$OUT/raw/kafka-read.raw" || log "lectura del topic con errores"
  # key<TAB>sobre JSON; se queda con los registros del prefijo
  cut -f2- "$OUT/raw/kafka-read.raw" \
    | jq -R -r --arg p "$_pfx" 'fromjson? | select(((.payload.sellerMeterId // "") | startswith($p)) or ((.payload.buyerMeterId // "") | startswith($p))) | [.payload.matchId, .eventId] | @csv' 2>/dev/null \
    | tr -d '"' >>"$_dest" || true
}

# recovery_s <crash_ts> <samples.csv>: segundos desde el crash hasta la primera muestra con pending == 0
recovery_s() {
  _first=$(awk -F, -v c="$1" 'NR > 1 && $1 >= c && $2 == 0 { print $1; exit }' "$2")
  [ -n "$_first" ] || { echo null; return 0; }
  expctl elapsed "$1" "$_first"
}

# --- una fase ------------------------------------------------------------------------------

# e6_phase <n> <outbox|direct> <failpoint> <segundos de carga> <crashes máx> <intentos de lectura de Kafka>
e6_phase() {
  _n=$1 _mode=$2 _fp=$3 _secs=$4 _max=$5 _tries=$6
  _pfx=e06-$RUN-p$_n-
  _tag=p$_n
  log "E6 fase $_n: modo=$_mode failpoint=$_fp=crash-after:$E6_CRASH_N, carga pairs $E6_RATE pares/s x ${_secs}s (prefijo $_pfx)"
  msets "${_tag}_mode" "$_mode"
  msets "${_tag}_failpoint" "$_fp"
  msets "${_tag}_prefix" "$_pfx"
  if [ "$_mode" = direct ]; then
    set_mode direct || return 1
    engine_started_with direct || { log "el motor no arrancó con outboxMode=direct"; return 1; }
  fi
  e56_wait 120 pending_zero || return 1
  dev_state >"$OUT/raw/$_tag-device-before.json" || return 1

  e56_bg sampler "$OUT/raw/$_tag-samples.csv" "$_pfx"
  _pid_s=$E56_LAST
  e56_bg e56_rearm_loop "$NS" "$ENG" "$_fp" "crash-after:$E6_CRASH_N" "$_max" "$OUT/raw/$_tag-crashes.csv"
  _pid_a=$E56_LAST
  sleep 2
  metersim_run r1 --profile pairs --meters $((E6_RATE * 2 * _secs)) --rate $((E6_RATE * 2)) --duration "${_secs}s" \
    --prefix "$_pfx" --seed 11 >"$OUT/raw/$_tag-load.log" 2>&1 || { log "meter-sim falló"; return 1; }
  metersim_summary <"$OUT/raw/$_tag-load.log" >"$OUT/raw/$_tag-load.json"
  mset "${_tag}_expected_pairs" "$(jq .expectedPairs "$OUT/raw/$_tag-load.json")"

  # drenaje: matches estables, outbox vacía y (con outbox) todos los efectos en device-sim
  e56_wait "$E6_DRAIN_S" matches_stable "$_pfx" "$OUT/raw/$_tag.count" || return 1
  e56_wait 60 matches_stable "$_pfx" "$OUT/raw/$_tag.count" || return 1
  psql_m "SELECT match_id FROM matches WHERE $(in_prefix "$_pfx") ORDER BY 1" >"$OUT/raw/$_tag-matches_ids.txt"
  if [ "$_mode" = outbox ]; then
    e56_wait 120 pending_zero || log "la outbox no quedó vacía"
    e56_wait "$E6_DRAIN_S" device_has_all "$OUT/raw/$_tag-matches_ids.txt" || log "device-sim no aplicó todos los matches"
  else
    sleep 30 # el control no tiene outbox que esperar: se da tiempo a lo que sí llegó a Kafka
  fi
  e56_kill "$_pid_a"
  e56_kill "$_pid_s"
  e56_disarm "$NS" "$ENG" "$_fp"

  # --- métricas de la fase ---
  mset "${_tag}_matches" "$(wc -l <"$OUT/raw/$_tag-matches_ids.txt" | tr -d ' ')"
  mset "${_tag}_orphans" "$(psql_m "SELECT count(*) FROM matches m LEFT JOIN outbox o ON o.key = m.match_id WHERE o.id IS NULL AND $(in_prefix "$_pfx" m.)")"
  mset "${_tag}_outbox_pending_final" "$(psql_m "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
  mset "${_tag}_crashes" "$(($(wc -l <"$OUT/raw/$_tag-crashes.csv") - 1))"

  # topic: ids distintos publicados con el prefijo (se reintenta la lectura mientras falte alguno)
  _t=0
  _dupev=0
  : >"$OUT/raw/$_tag-kafka_all.csv"
  while [ "$_t" -lt "$_tries" ]; do
    kafka_read "$_pfx" "$OUT/raw/$_tag-kafka_read.csv"
    cat "$OUT/raw/$_tag-kafka_read.csv" >>"$OUT/raw/$_tag-kafka_all.csv"
    # duplicados en el broker: eventId en más de un registro de la misma lectura (la mayor entre lecturas)
    _d=$(awk -F, '{ c[$2]++ } END { for (e in c) if (c[e] > 1) n++; print n + 0 }' "$OUT/raw/$_tag-kafka_read.csv")
    [ "$_d" -le "$_dupev" ] || _dupev=$_d
    cut -d, -f1 "$OUT/raw/$_tag-kafka_all.csv" | sort -u >"$OUT/raw/$_tag-kafka_event_ids.txt"
    _lost=$(comm -23 "$OUT/raw/$_tag-matches_ids.txt" "$OUT/raw/$_tag-kafka_event_ids.txt" | wc -l | tr -d ' ')
    _t=$((_t + 1))
    [ "$_mode" = outbox ] && [ "$_lost" = 0 ] && break
  done
  # kafka_read devuelve matchId,eventId por registro; las lecturas se unen (un registro leído dos veces es la misma entrega)
  sort -u "$OUT/raw/$_tag-kafka_all.csv" >"$OUT/raw/$_tag-kafka_records.csv"
  comm -23 "$OUT/raw/$_tag-matches_ids.txt" "$OUT/raw/$_tag-kafka_event_ids.txt" >"$OUT/raw/$_tag-lost_ids.txt"
  comm -13 "$OUT/raw/$_tag-matches_ids.txt" "$OUT/raw/$_tag-kafka_event_ids.txt" >"$OUT/raw/$_tag-spurious_ids.txt"
  mset "${_tag}_kafka_distinct_matchids" "$(wc -l <"$OUT/raw/$_tag-kafka_event_ids.txt" | tr -d ' ')"
  mset "${_tag}_lost" "$(wc -l <"$OUT/raw/$_tag-lost_ids.txt" | tr -d ' ')"
  mset "${_tag}_spurious" "$(wc -l <"$OUT/raw/$_tag-spurious_ids.txt" | tr -d ' ')"
  mset "${_tag}_kafka_dup_events" "$_dupev"

  # efectos: device-sim por matchId
  dev_state >"$OUT/raw/$_tag-device-after.json" || return 1
  mset "${_tag}_device_missing" "$(jq --rawfile ids "$OUT/raw/$_tag-matches_ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | (.byMatchId[$i] // 0)] | map(select(. == 0)) | length' "$OUT/raw/$_tag-device-after.json")"
  mset "${_tag}_device_duplicated" "$(jq --rawfile ids "$OUT/raw/$_tag-matches_ids.txt" '[($ids | split("\n") | map(select(length > 0)))[] as $i | (.byMatchId[$i] // 0)] | map(select(. > 1)) | length' "$OUT/raw/$_tag-device-after.json")"
  mset "${_tag}_device_dups_delta" "$(( $(jq .duplicates "$OUT/raw/$_tag-device-after.json") - $(jq .duplicates "$OUT/raw/$_tag-device-before.json") ))"
  mset "${_tag}_transfers_dup_groups" "$(psql_tr "SELECT count(*) FROM (SELECT match_id FROM transfers WHERE $(in_prefix "$_pfx") GROUP BY 1 HAVING count(*) > 1) g")"

  # recuperación por crash: del fin del contenedor a la primera muestra con outbox vacía
  _max_rec=0
  _rec_json="["
  _sep=""
  if [ "$(wc -l <"$OUT/raw/$_tag-crashes.csv")" -gt 1 ]; then
    awk -F, 'NR > 1 { print $1 }' "$OUT/raw/$_tag-crashes.csv" >"$OUT/raw/$_tag-crash-ts.txt"
    while read -r _ts; do
      _r=$(recovery_s "$_ts" "$OUT/raw/$_tag-samples.csv")
      _rec_json="$_rec_json$_sep{\"crashAt\":\"$_ts\",\"recoverySec\":$_r}"
      _sep=","
      case $_r in null) _max_rec=null ;; *) [ "$_max_rec" = null ] || _max_rec=$(awk -v a="$_max_rec" -v b="$_r" 'BEGIN { print (b > a) ? b : a }') ;; esac
    done <"$OUT/raw/$_tag-crash-ts.txt"
  fi
  echo "$_rec_json]" >"$OUT/raw/$_tag-recovery.json"
  [ "$(wc -l <"$OUT/raw/$_tag-crashes.csv")" -gt 1 ] || _max_rec=null
  mset "${_tag}_recovery_max_s" "$_max_rec"

  if [ "$_mode" = direct ]; then
    set_mode outbox || return 1
  fi
  return 0
}

drive() {
  e6_phase 1 outbox after_commit_before_publish "$E6_S" "$E6_CRASHES" "$E6_KAFKA_TRIES" || return 1
  e6_phase 2 outbox after_publish_before_mark "$E6_S" "$E6_CRASHES" "$E6_KAFKA_TRIES" || return 1
  e6_phase 3 direct after_commit_before_publish "$E6_CONTROL_S" 1 2 || return 1
}

# --- collect -------------------------------------------------------------------------------

collect() {
  _t0=$1 _t1=$2 _dir=$3
  # outbox_before_after.csv: pendientes y matches al inicio, en el pico y al final de cada fase
  echo "phase,mode,failpoint,matches,orphans,pending_max,pending_final,crashes,recovery_max_s,kafka_distinct,lost,spurious,kafka_dup_events,device_missing,device_duplicated" >"$_dir/outbox_before_after.csv"
  for _n in 1 2 3; do
    jq -r --arg n "$_n" --argjson pmax "$(awk -F, 'NR > 1 && $2 + 0 > m { m = $2 + 0 } END { print m + 0 }' "$_dir/raw/p$_n-samples.csv" 2>/dev/null || echo 0)" '
      "p\($n),\(.["p\($n)_mode"]),\(.["p\($n)_failpoint"]),\(.["p\($n)_matches"]),\(.["p\($n)_orphans"]),\($pmax),\(.["p\($n)_outbox_pending_final"]),\(.["p\($n)_crashes"]),\(.["p\($n)_recovery_max_s"]),\(.["p\($n)_kafka_distinct_matchids"]),\(.["p\($n)_lost"]),\(.["p\($n)_spurious"]),\(.["p\($n)_kafka_dup_events"]),\(.["p\($n)_device_missing"]),\(.["p\($n)_device_duplicated"])"' \
      "$_dir/raw/m.json" >>"$_dir/outbox_before_after.csv" 2>/dev/null || true
    cp "$_dir/raw/p$_n-kafka_event_ids.txt" "$_dir/kafka_event_ids-p$_n.txt" 2>/dev/null || true
    cp "$_dir/raw/p$_n-matches_ids.txt" "$_dir/matches_ids-p$_n.txt" 2>/dev/null || true
  done
  jq --arg t0 "$_t0" --arg t1 "$_t1" '. + {tStart: $t0, tEnd: $t1}' "$_dir/raw/m.json" >"$_dir/metrics.json"
}
