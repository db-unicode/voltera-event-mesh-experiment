#!/bin/sh
# E2 parte A: KEDA sobre RabbitMQ (declaration-gateway 0 -> N -> 0) y control con 1 réplica fija.
# Se carga con `.` desde experiments/run.sh (define drive y collect).
# shellcheck shell=sh disable=SC2034,SC2329  # las funciones las invoca el runner
# shellcheck source=experiments/lib/metersim.sh
. "$HERE/lib/metersim.sh"
# shellcheck source=experiments/lib/loadexp.sh
. "$HERE/lib/loadexp.sh"

E2_NS=voltera-r1
E2_GW=declaration-gateway
E2_SPIKE_S=${E2_SPIKE_S:-60}
E2_IDLE_S=${E2_IDLE_S:-30}                 # tiempo mínimo con el gateway en 0 antes del pico
E2_BASELINE_S=${E2_BASELINE_S:-10}         # muestras previas al pico
E2_DRAIN_LIMIT_S=${E2_DRAIN_LIMIT_S:-660}  # tope de espera del drenado (el control tarda ~6 min)
E2_SCALE_IN_LIMIT_S=${E2_SCALE_IN_LIMIT_S:-300}
E2_METERS=$((SPIKE_RATE * E2_SPIKE_S + 2000))   # un medidor distinto por declaración: filas en positions-db == enviadas
E2_TARGET=$(( (8 * SPIKE_RATE + 10 * C_POD - 1) / (10 * C_POD) ))   # ceil(0.8 x SPIKE_RATE / C_POD)
[ "$E2_TARGET" -le "$KEDA_MAX_REPLICAS" ] || E2_TARGET=$KEDA_MAX_REPLICAS

e2_cleanup() {
  lx_stop
  chaos_resume_keda "$E2_NS" "$E2_GW" >/dev/null 2>&1 || true
}
trap 'e2_cleanup; cluster_unlock' EXIT INT TERM

E2_COLS=depth,gw,gw_ready,posq
e2_row() {
  _d=$(lx_depth r1 meter-declarations)
  _r=$(lx_replicas "$E2_NS" "$E2_GW")
  _q=$(lx_depth r1 energy-positions)
  echo "$_d,$_r,$_q"
}

e2_idle_gw() {
  [ "$(lx_depth r1 meter-declarations)" = 0 ] \
    && [ "$(lx_replicas "$E2_NS" "$E2_GW")" = "0,0" ] \
    && [ "$(lx_sql "$E2_NS/positions-db" "select count(*) from outbox where published_at is null")" = 0 ]
}

e2_gw_pinned_one() {
  [ "$(lx_replicas "$E2_NS" "$E2_GW")" = "1,1" ] && [ "$(lx_depth r1 meter-declarations)" = 0 ]
}

e2_depth_le1() { [ "$(lx_depth r1 meter-declarations)" -le 1 ]; }
e2_depth_zero() { [ "$(lx_depth r1 meter-declarations)" = 0 ]; }
e2_posq_zero() { [ "$(lx_depth r1 energy-positions)" = 0 ]; }
e2_gw_zero() { [ "$(lx_replicas "$E2_NS" "$E2_GW")" = "0,0" ]; }

exp_reset() {
  mkdir -p "$OUT/logs"
  chaos_resume_keda "$E2_NS" "$E2_GW" >/dev/null 2>&1 || true
  lx_open_forwards || return 1
  log "esperando gateway en 0 réplicas y cola vacía"
  lx_wait 300 e2_idle_gw || { log "el gateway no llegó a 0 con la cola vacía"; return 1; }
}

# e2_phase <tag> <keda|control>
e2_phase() {
  _tag=$1 _mode=$2
  E2_PREFIX="e02$_tag-$(lx_now)-"
  if [ "$_mode" = keda ]; then
    chaos_resume_keda "$E2_NS" "$E2_GW" >/dev/null 2>&1 || true
    lx_wait 300 e2_idle_gw || return 1
    log "fase $_tag (KEDA): gateway en 0 durante $E2_IDLE_S s"
    sleep "$E2_IDLE_S"
    e2_gw_zero || { log "el gateway no sigue en 0"; return 1; }
  else
    log "fase $_tag (control): KEDA pausado con 1 réplica fija"
    chaos_pause_keda "$E2_NS" "$E2_GW" 1
    lx_wait 180 e2_gw_pinned_one || { log "el gateway no quedó en 1 réplica lista"; return 1; }
  fi
  lx_sample "$OUT/raw/depth-$_tag.csv" 3000 "$E2_COLS" e2_row &
  _s1=$!
  expctl sample --interval 2s --duration 3000s --out "$OUT/raw/replicas-$_tag.csv" \
    --replicas "gw=$E2_NS/$E2_GW" \
    --prom 'depth_prom=rabbitmq_queue_messages_ready{vhost="r1",queue="meter-declarations"}' \
    --prom 'unacked_prom=rabbitmq_queue_messages_unacked{vhost="r1",queue="meter-declarations"}' 2>"$OUT/logs/sample-$_tag.log" &
  _s2=$!
  sleep "$E2_BASELINE_S"
  log "fase $_tag: pico de $SPIKE_RATE msg/s durante ${E2_SPIKE_S}s ($E2_METERS medidores, prefijo $E2_PREFIX)"
  METERSIM_DEADLINE=300 metersim_run r1 --profile spike --rate "$SPIKE_RATE" --duration "${E2_SPIKE_S}s" --meters "$E2_METERS" \
    --prefix "$E2_PREFIX" --horizon 5s --seed 2 >"$OUT/logs/metersim-$_tag.log" 2>&1 \
    || log "meter-sim terminó con error (ver logs/metersim-$_tag.log)"
  metersim_summary <"$OUT/logs/metersim-$_tag.log" >"$OUT/raw/spike-$_tag.json"
  _t_end=$(lx_now)
  log "fase $_tag: pico terminado; esperando profundidad <= 1 (máx ${E2_DRAIN_LIMIT_S}s)"
  _drained=1
  lx_wait "$E2_DRAIN_LIMIT_S" e2_depth_le1 || _drained=0
  _t_drain=$(lx_now)
  lx_wait 120 e2_depth_zero || true
  _zeroed=1
  if [ "$_mode" = keda ]; then
    log "fase $_tag: esperando scale-to-zero (máx ${E2_SCALE_IN_LIMIT_S}s)"
    lx_wait "$E2_SCALE_IN_LIMIT_S" e2_gw_zero || _zeroed=0
  fi
  _t_zero=$(lx_now)
  lx_wait 120 e2_idle_gw >/dev/null 2>&1 || true
  if [ "$_mode" = keda ]; then
    k -n "$E2_NS" get scaledobject "$E2_GW" -o json >"$OUT/raw/keda-$_tag-scaledobject.json" || true
    k -n "$E2_NS" get hpa -o json >"$OUT/raw/keda-$_tag-hpa.json" || true
    k -n "$E2_NS" get events --sort-by=.lastTimestamp -o json \
      | jq '[.items[] | select(.involvedObject.name | test("declaration-gateway"))
             | {time: (.lastTimestamp // .eventTime), type, reason, object: .involvedObject.name, message}]' >"$OUT/raw/keda-$_tag-events.json" || true
  fi
  sleep 4
  kill "$_s1" "$_s2" 2>/dev/null || true
  jq -n --arg prefix "$E2_PREFIX" --arg mode "$_mode" --argjson tEnd "$_t_end" --argjson tDrain "$_t_drain" \
    --argjson tZero "$_t_zero" --argjson drained "$_drained" --argjson zeroed "$_zeroed" \
    '{prefix: $prefix, mode: $mode, tEnd: $tEnd, tDrainWait: $tDrain, tZeroWait: $tZero, drained: ($drained == 1), zeroed: ($zeroed == 1)}' \
    >"$OUT/raw/phase-$_tag.json"
}

drive() {
  _rc=0
  e2_phase k keda || _rc=1
  chaos_resume_keda "$E2_NS" "$E2_GW" >/dev/null 2>&1 || true
  lx_wait 300 e2_idle_gw || log "el gateway no volvió a 0 entre fases"
  e2_phase c control || _rc=1
  chaos_resume_keda "$E2_NS" "$E2_GW" >/dev/null 2>&1 || true
  # deja el sistema como estaba: gateway en 0 y cola de posiciones drenada por el proyector
  lx_wait 300 e2_idle_gw || log "el gateway no volvió a 0 al terminar"
  lx_wait 300 e2_posq_zero || log "energy-positions no se vació"
  return "$_rc"
}

# ---- recolección ----

# e2_series <csv> <t_inicio> <t_fin_pico> <objetivo>
# Imprime: pico de profundidad, pico de réplicas (spec), pico de listas, t_scale_up (ready >= objetivo; -1 si nunca),
# t_drain (primera fila >= t_fin_pico con profundidad <= 1; -1), t_zero (primera fila >= t_drain con gw = 0; -1),
# bajadas de réplicas durante el pico
e2_series() {
  awk -F, -v t0="$2" -v t1="$3" -v tgt="$4" "$LX_EPOCH_AWK"'
    BEGIN { pd = 0; pr = 0; pk = 0; su = -1; dr = -1; zr = -1; downs = 0; prev = -1 }
    NR == 1 { for (i = 1; i <= NF; i++) { if ($i == "depth") di = i; if ($i == "gw") gi = i; if ($i == "gw_ready") ri = i }; next }
    $di == "" || $gi == "" { next }
    { t = epoch($1); d = $di + 0; g = $gi + 0; r = $ri + 0
      if (d > pd) pd = d
      if (g > pr) pr = g
      if (r > pk) pk = r
      if (t >= t0 && su < 0 && r >= tgt) su = t
      if (t >= t0 && t <= t1) { if (prev >= 0 && g < prev) downs++ }
      prev = g
      if (t >= t1 && dr < 0 && d <= 1) dr = t
      if (dr >= 0 && t >= dr && zr < 0 && g == 0) zr = t }
    END { printf "%d %d %d %d %d %d %d\n", pd, pr, pk, su, dr, zr, downs }' "$1"
}

e2_phase_metrics() {
  _tag=$1
  _ph="$OUT/raw/phase-$_tag.json"
  _sp="$OUT/raw/spike-$_tag.json"
  _prefix=$(jq -r .prefix "$_ph")
  _t_end_wait=$(jq -r .tEnd "$_ph")
  _started=$(jq -r .startedAt "$_sp")
  _ended=$(jq -r .endedAt "$_sp")
  _t0=$(jq -r '.startedAt | sub("\\.[0-9]+"; "") | fromdate' "$_sp")
  _t1=$(jq -r '.endedAt | sub("\\.[0-9]+"; "") | fromdate' "$_sp")
  # shellcheck disable=SC2046  # la salida son enteros separados por espacios
  set -- $(e2_series "$OUT/raw/depth-$_tag.csv" "$_t0" "$_t1" "$E2_TARGET")
  _pd=$1 _pr=$2 _pk=$3 _su=$4 _dr=$5 _zr=$6 _downs=$7
  _scale_up=-1 _drain=-1 _zero=-1
  [ "$_su" -ge 0 ] && _scale_up=$((_su - _t0))
  [ "$_dr" -ge 0 ] && _drain=$((_dr - _t1))
  [ "$_drain" -ge 0 ] || _drain=-1
  [ "$_dr" -ge 0 ] && [ "$_zr" -ge 0 ] && _zero=$((_zr - _dr))
  _rows=$(lx_sql "$E2_NS/positions-db" "select count(*) from positions where meter_id like '${_prefix}%'")
  _outbox=$(lx_sql "$E2_NS/positions-db" "select count(*) from outbox where aggregate_id like '${_prefix}%'")
  _cold=$(lx_sql "$E2_NS/positions-db" "select coalesce(round(extract(epoch from min(updated_at) - '$_started'::timestamptz)::numeric, 2), -1) from positions where meter_id like '${_prefix}%'")
  _p99=$(lx_sql "$E2_NS/positions-db" "select coalesce(round(percentile_cont(0.99) within group (order by extract(epoch from created_at - (payload->>'occurredAt')::timestamptz))::numeric, 2), -1) from outbox where aggregate_id like '${_prefix}%'")
  _p50=$(lx_sql "$E2_NS/positions-db" "select coalesce(round(percentile_cont(0.5) within group (order by extract(epoch from created_at - (payload->>'occurredAt')::timestamptz))::numeric, 2), -1) from outbox where aggregate_id like '${_prefix}%'")
  jq -n --slurpfile ph "$_ph" --slurpfile sp "$_sp" --argjson pd "$_pd" --argjson pr "$_pr" --argjson pk "$_pk" \
    --argjson scaleUp "$_scale_up" --argjson drain "$_drain" --argjson zero "$_zero" --argjson downs "$_downs" \
    --argjson rows "$_rows" --argjson outbox "$_outbox" --argjson cold "$_cold" --argjson p99 "$_p99" --argjson p50 "$_p50" \
    --argjson limit "$E2_DRAIN_LIMIT_S" '
    {mode: $ph[0].mode, prefix: $ph[0].prefix, drained: $ph[0].drained, zeroed: $ph[0].zeroed,
     sent: $sp[0].published, planned: $sp[0].planned, errors: $sp[0].errors, dropped: $sp[0].dropped, achievedRate: $sp[0].achievedRate,
     positionsRows: $rows, outboxRows: $outbox, loss: ($sp[0].published - $rows),
     coldStartS: $cold, timeToScaleUpS: $scaleUp, peakDepth: $pd, peakReplicas: $pr, peakReady: $pk,
     drainS: $drain, drainSEffective: (if $drain < 0 then $limit else $drain end), scaleToZeroS: $zero,
     queueWaitP50S: $p50, queueWaitP99S: $p99, replicaDropsDuringSpike: $downs}' >"$OUT/raw/metrics-$_tag.json"
}

# e2_merge: CSV de la figura KEDA vs control en el mismo eje (t desde el inicio del pico de cada fase)
e2_merge() {
  _out="$OUT/figures/keda_vs_control.csv"
  echo "t_s,depth_keda,replicas_keda_x100,depth_control,replicas_control_x100" >"$_out"
  for _t in k c; do
    _t0=$(jq -r '.startedAt | sub("\\.[0-9]+"; "") | fromdate' "$OUT/raw/spike-$_t.json")
    awk -F, -v t0="$_t0" -v tag="$_t" "$LX_EPOCH_AWK"'
      NR == 1 { next }
      $2 == "" || $3 == "" { next }
      { t = epoch($1) - t0
        if (tag == "k") print t "," $2 "," $3 * 100 ",,"
        else print t ",,," $2 "," $3 * 100 }' "$OUT/raw/depth-$_t.csv" >>"$_out"
  done
  expctl plot --csv "$_out" --x t_s --title "E2A: profundidad de meter-declarations y réplicas x100 (KEDA vs control)" \
    --out "$OUT/figures/e02a-keda-vs-control.png" || log "sin figura KEDA vs control"
}

collect() {
  _rc=0
  for _t in k c; do
    if [ -f "$OUT/raw/phase-$_t.json" ] && [ -s "$OUT/raw/spike-$_t.json" ]; then
      e2_phase_metrics "$_t" || { log "falló la recolección de la fase $_t"; _rc=1; }
    else
      log "falta la fase $_t"
      _rc=1
    fi
  done
  [ -f "$OUT/raw/metrics-k.json" ] && [ -f "$OUT/raw/metrics-c.json" ] || return 1
  jq -n --slurpfile k "$OUT/raw/metrics-k.json" --slurpfile c "$OUT/raw/metrics-c.json" \
    --argjson target "$E2_TARGET" --argjson max "$KEDA_MAX_REPLICAS" --argjson rate "$SPIKE_RATE" --argjson cpod "$C_POD" '
    {params: {spikeRate: $rate, cPod: $cpod, targetReplicas: $target, maxReplicas: $max},
     keda: ($k[0] + {peakMargin: ($k[0].peakReplicas - $target)}), control: $c[0],
     drainRatio: (if $k[0].drainSEffective > 0 then ($c[0].drainSEffective / $k[0].drainSEffective) else 0 end)}' >"$OUT/metrics.json" || return 1
  e2_merge || true
  return "$_rc"
}
