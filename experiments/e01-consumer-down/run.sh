#!/bin/sh
# E1: consumidor caído, el emisor no se entera. Se carga con `.` desde experiments/run.sh (define drive y collect).
#   Fase a: energy-transfer a 0 réplicas (paused-replicas=0) 60 s bajo 20 pares/s.
#   Fase b: market-projector a 0 réplicas 60 s (< TTL) bajo la misma carga.
# shellcheck shell=sh disable=SC2034,SC2329  # las funciones las invoca el runner
# shellcheck source=experiments/lib/metersim.sh
. "$HERE/lib/metersim.sh"
# shellcheck source=experiments/lib/loadexp.sh
. "$HERE/lib/loadexp.sh"

E1_NS=voltera-r1
E1_TOPIC=r1.energy-matched
E1_PAIRS_RATE=${E1_PAIRS_RATE:-20}          # pares/s
E1_LOAD_S=${E1_LOAD_S:-180}
E1_PAUSE_AT_S=${E1_PAUSE_AT_S:-30}
E1_DOWN_S=${E1_DOWN_S:-60}
E1_METERS_A=${E1_METERS_A:-1200}            # vuelta de cada medidor cada meters/(2*rate) = 30 s
E1_METERS_B=${E1_METERS_B:-400}             # 10 s: todos los medidores están en la vista antes de la caída
E1_HORIZON_A=${E1_HORIZON_A:-30}            # vigencia de cada posición (s); en b > D para que lo encolado siga siendo emparejable
E1_HORIZON_B=${E1_HORIZON_B:-90}
E1_SETTLE_S=${E1_SETTLE_S:-300}
E1_PROJ_REPLICAS=1

e1_cleanup() {
  lx_stop
  chaos_resume_keda "$E1_NS" energy-transfer >/dev/null 2>&1 || true
  chaos_resume_keda "$E1_NS" declaration-gateway >/dev/null 2>&1 || true
  k -n "$E1_NS" scale deploy market-projector --replicas="$E1_PROJ_REPLICAS" >/dev/null 2>&1 || true
}
trap 'e1_cleanup; cluster_unlock' EXIT INT TERM

# ---- series ----

E1_COLS=lag,et,et_ready,gw,gw_ready,proj,proj_ready,posq
e1_row() {
  _lag=$(lx_lag energy-transfer "$E1_TOPIC")
  _d=$(k -n "$E1_NS" get deploy energy-transfer declaration-gateway market-projector -o json \
    | jq -r '.items | map({key: .metadata.name, value: [(.spec.replicas // 0), (.status.readyReplicas // 0)]}) | from_entries
             | [.["energy-transfer"][], .["declaration-gateway"][], .["market-projector"][]] | join(",")')
  _q=$(lx_depth r1 energy-positions)
  echo "$_lag,$_d,$_q"
}

# keys de la vista del prefijo en curso (E1_PREFIX)
e1_keys_row() {
  k -n "$E1_NS" exec deploy/market-view -c redis -- redis-cli --scan --pattern "pos:*:${E1_PREFIX}*" | wc -l | tr -d ' '
}

# ---- consultas de base de datos ----

e1_where() { printf "seller_meter_id like '%s%%' or buyer_meter_id like '%s%%'" "$1" "$1"; }

e1_first_row() { [ "$(lx_sql "$E1_NS/positions-db" "select count(*) from positions where meter_id like '${E1_PREFIX}%'")" -gt 0 ]; }

# e1_effects <tag> <prefix> <salida.json>: matches del prefijo vs efectos en device-sim y billing-sim (por matchId)
e1_effects() {
  expctl sql "$E1_NS/matches-db" "select match_id from matches where $(e1_where "$2") order by 1" --no-header \
    | tr -d '"' | jq -R . | jq -s . >"$OUT/raw/ids-$1.json" || return 1
  lx_state "$LX_PF_DEVICE" "$OUT/raw/device-$1.json" || return 1
  lx_state "$LX_PF_BILLING" "$OUT/raw/billing-$1.json" detail || return 1
  jq -n --slurpfile ids "$OUT/raw/ids-$1.json" --slurpfile dev "$OUT/raw/device-$1.json" --slurpfile bil "$OUT/raw/billing-$1.json" '
    $ids[0] as $I
    | {matches: ($I | length),
       device:  {missing: [$I[] as $i | select(($dev[0].byMatchId[$i] // 0) == 0)] | length,
                 dups:    [$I[] as $i | select(($dev[0].byMatchId[$i] // 0) > 1)] | length},
       billing: {missing: [$I[] as $i | select(($bil[0].byMatchId[$i].count // 0) == 0)] | length,
                 dups:    [$I[] as $i | select(($bil[0].byMatchId[$i].count // 0) > 1)] | length}}' >"$3"
}

# e1_settled <tag> <prefix>: todo drenado (lag 0, outbox vacías, cola 0, efectos completos)
e1_settled() {
  [ "$(lx_lag energy-transfer "$E1_TOPIC")" = 0 ] || return 1
  [ "$(lx_depth r1 energy-positions)" = 0 ] || return 1
  [ "$(lx_sql "$E1_NS/matches-db" "select count(*) from outbox where published_at is null")" = 0 ] || return 1
  [ "$(lx_sql "$E1_NS/positions-db" "select count(*) from outbox where published_at is null")" = 0 ] || return 1
  _m=$(lx_sql "$E1_NS/matches-db" "select count(*) from matches where $(e1_where "$2")")
  _s=$(lx_sql "$E1_NS/transfer-db" "select count(*) from transfers where status = 'SENT' and ($(e1_where "$2"))")
  [ "$_m" = "$_s" ] || return 1
  e1_effects "$1" "$2" "$OUT/raw/effects-$1.json" || return 1
  jq -e '.matches == '"$_m"' and .device.missing == 0 and .billing.missing == 0' "$OUT/raw/effects-$1.json" >/dev/null
}

e1_gw_one() { [ "$(lx_replicas "$E1_NS" declaration-gateway)" = "1,1" ]; }

e1_idle() {
  [ "$(lx_lag energy-transfer "$E1_TOPIC")" = 0 ] \
    && [ "$(lx_depth r1 energy-positions)" = 0 ] \
    && [ "$(lx_depth r1 meter-declarations)" = 0 ] \
    && [ "$(lx_sql "$E1_NS/matches-db" "select count(*) from outbox where published_at is null")" = 0 ] \
    && [ "$(lx_sql "$E1_NS/positions-db" "select count(*) from outbox where published_at is null")" = 0 ]
}

exp_reset() {
  mkdir -p "$OUT/logs"
  chaos_resume_keda "$E1_NS" energy-transfer >/dev/null 2>&1 || true
  chaos_resume_keda "$E1_NS" declaration-gateway >/dev/null 2>&1 || true
  E1_PROJ_REPLICAS=$(k -n "$E1_NS" get deploy market-projector -o jsonpath='{.spec.replicas}')
  [ "$E1_PROJ_REPLICAS" -ge 1 ] || E1_PROJ_REPLICAS=1
  k -n "$E1_NS" scale deploy market-projector --replicas="$E1_PROJ_REPLICAS" >/dev/null
  lx_open_forwards || return 1
  log "esperando sistema en reposo (lag 0, colas 0, outbox vacías)"
  lx_wait 240 e1_idle || { log "el sistema no quedó en reposo"; return 1; }
}

# ---- una fase ----

# e1_down / e1_up <kind>: transfer = KEDA pausado en 0; projector = deployment a 0 y de vuelta
e1_down() {
  case $1 in
    transfer) chaos_pause_keda "$E1_NS" energy-transfer 0 ;;
    projector) k -n "$E1_NS" scale deploy market-projector --replicas=0 >/dev/null ;;
  esac
}
e1_up() {
  case $1 in
    transfer) chaos_resume_keda "$E1_NS" energy-transfer ;;
    projector) k -n "$E1_NS" scale deploy market-projector --replicas="$E1_PROJ_REPLICAS" >/dev/null ;;
  esac
}

# e1_phase <tag> <transfer|projector> <medidores> <horizon s>
e1_phase() {
  _tag=$1 _kind=$2 _meters=$3 _horizon=$4
  E1_PREFIX="e01$_tag-$(lx_now)-"
  log "fase $_tag ($_kind caído): prefijo $E1_PREFIX, $E1_PAIRS_RATE pares/s, $_meters medidores"
  if [ "$_kind" = projector ]; then
    # el emisor de la fase b es el gateway: 1 réplica fija (sin cold start ni reescalado) para comparar su p95 antes/durante
    chaos_pause_keda "$E1_NS" declaration-gateway 1
    lx_wait 180 e1_gw_one || log "el gateway no quedó en 1 réplica lista"
    sleep 20   # al menos un scrape de Prometheus del pod nuevo
  fi
  _dev0=$(curl -fsS -m 20 "http://127.0.0.1:$LX_PF_DEVICE/state" | jq -c '{total, distinct, duplicates}')
  _bil0=$(curl -fsS -m 20 "http://127.0.0.1:$LX_PF_BILLING/state" | jq -c '.byRegion.r1 | {total, distinct, duplicates}')
  _t_start=$(lx_now)
  lx_sample "$OUT/raw/series-$_tag.csv" 3000 "$E1_COLS" e1_row &
  _sampler=$!
  _keys=
  if [ "$_kind" = projector ]; then
    lx_sample "$OUT/raw/keys-$_tag.csv" 3000 keys e1_keys_row &
    _keys=$!
  fi
  metersim_run r1 --profile pairs --rate $((E1_PAIRS_RATE * 2)) --duration "${E1_LOAD_S}s" --meters "$_meters" \
    --prefix "$E1_PREFIX" --horizon "${_horizon}s" --seed 1 >"$OUT/logs/metersim-$_tag.log" 2>&1 &
  _mpid=$!
  lx_wait 120 e1_first_row || { log "meter-sim no llegó a positions-db"; kill "$_sampler" $_keys 2>/dev/null; return 1; }
  _t_load0=$(lx_now)
  sleep "$E1_PAUSE_AT_S"
  e1_down "$_kind"
  _t_pause=$(lx_now)
  sleep "$E1_DOWN_S"
  e1_up "$_kind"
  _t_resume=$(lx_now)
  wait "$_mpid" || log "meter-sim terminó con error (ver logs/metersim-$_tag.log)"
  _t_load_end=$(lx_now)
  log "carga terminada; esperando drenado completo (máx ${E1_SETTLE_S}s)"
  sleep $((_horizon + 15))   # tras la vigencia de la última posición el motor ya no empareja nada más
  _ok=1
  lx_wait "$E1_SETTLE_S" e1_settled "$_tag" "$E1_PREFIX" || _ok=0
  _t_settled=$(lx_now)
  sleep 4   # una última fila de las series
  kill "$_sampler" $_keys 2>/dev/null || true
  [ "$_ok" = 1 ] || log "fase $_tag: el sistema no se asentó en ${E1_SETTLE_S}s"
  [ "$_kind" != projector ] || chaos_resume_keda "$E1_NS" declaration-gateway
  jq -n --arg prefix "$E1_PREFIX" --arg kind "$_kind" --argjson dev0 "$_dev0" --argjson bil0 "$_bil0" \
    --argjson tStart "$_t_start" --argjson tLoad0 "$_t_load0" --argjson tPause "$_t_pause" --argjson tResume "$_t_resume" \
    --argjson tLoadEnd "$_t_load_end" --argjson tSettled "$_t_settled" --argjson settled "$_ok" \
    '{prefix: $prefix, kind: $kind, device0: $dev0, billing0: $bil0, tStart: $tStart, tLoad0: $tLoad0, tPause: $tPause,
      tResume: $tResume, tLoadEnd: $tLoadEnd, tSettled: $tSettled, settled: ($settled == 1)}' >"$OUT/raw/phase-$_tag.json"
  [ "$_ok" = 1 ]
}

drive() {
  _rc=0
  e1_phase a transfer "$E1_METERS_A" "$E1_HORIZON_A" || _rc=1
  log "pausa entre fases (sistema en reposo)"
  lx_wait 240 e1_idle || log "el sistema no volvió al reposo entre fases"
  e1_phase b projector "$E1_METERS_B" "$E1_HORIZON_B" || _rc=1
  return "$_rc"
}

# ---- recolección ----

# e1_series <csv> <columna> <columna-ready> <t_pause> <t_resume>
# Imprime: pico, t_pico, t_ready (primera fila >= t_resume con ready >= 1), t_cero (primer 0 tras el pico y el ready),
# t_down y valor en ella (primera fila >= t_pause con el consumidor sin pods Ready), t_up y valor en ella (última fila
# <= t_resume), valor final. Los instantes son los de las filas: el crecimiento y lo producido se miden entre las mismas dos filas.
e1_series() {
  awk -F, -v mcol="$2" -v rcol="$3" -v tp="$4" -v tr="$5" "$LX_EPOCH_AWK"'
    BEGIN { peak = -1; peakt = -1; rt = -1; zt = -1; td = -1; vd = -1; tu = -1; vu = -1; last = -1 }
    NR == 1 { for (i = 1; i <= NF; i++) { if ($i == mcol) mi = i; if ($i == rcol) ri = i }; next }
    $mi == "" { next }
    { t = epoch($1); v = $mi + 0
      last = v
      if (t >= tp && td < 0 && $ri + 0 == 0) { td = t; vd = v }
      if (t <= tr) { tu = t; vu = v }
      if (t >= tp && v > peak) { peak = v; peakt = t }
      if (t >= tr && rt < 0 && $ri + 0 >= 1) rt = t
      if (rt >= 0 && t >= rt && t >= peakt && zt < 0 && v == 0) zt = t }
    END { printf "%d %d %d %d %d %d %d %d %d\n", peak, peakt, rt, zt, td, vd, tu, vu, last }' "$1"
}

# e1_prom <consulta con %W% y %S%> <ventana s> <epoch>: valor (0 si no hay serie)
e1_prom() {
  _q=$(printf '%s' "$1" | sed "s/%W%/$2/g")
  _v=$(lx_promv "$_q" "$3")
  echo "${_v:-0}"
}

# e1_phase_metrics <tag>: escribe raw/metrics-<tag>.json
e1_phase_metrics() {
  _tag=$1
  _ph="$OUT/raw/phase-$_tag.json"
  _prefix=$(jq -r .prefix "$_ph")
  _kind=$(jq -r .kind "$_ph")
  _tp=$(jq -r .tPause "$_ph")
  _tr=$(jq -r .tResume "$_ph")
  _tl0=$(jq -r .tLoad0 "$_ph")
  _tle=$(jq -r .tLoadEnd "$_ph")
  _ts=$(jq -r .tSettled "$_ph")
  _tstart=$(jq -r .tStart "$_ph")
  _down=$((_tr - _tp))

  [ -s "$OUT/raw/effects-$_tag.json" ] || e1_effects "$_tag" "$_prefix" "$OUT/raw/effects-$_tag.json" || return 1
  _w=$(e1_where "$_prefix")
  if [ "$_kind" = transfer ]; then
    _series_col=lag
    _ready_col=et_ready
    _app=matching-engine
    _pq_hist='histogram_quantile(0.95, sum by (le) (increase(match_window_seconds_bucket{app="matching-engine",namespace="voltera-r1"}[%W%s])))'
    _rate_q='sum(increase(matches_created_total{app="matching-engine",namespace="voltera-r1"}[%W%s]))'
  else
    _series_col=posq
    _ready_col=proj_ready
    _app=declaration-gateway
    _pq_hist='histogram_quantile(0.95, sum by (le) (increase(event_processing_seconds_bucket{app="declaration-gateway",namespace="voltera-r1"}[%W%s])))'
    _rate_q='sum(increase(declarations_processed_total{app="declaration-gateway",namespace="voltera-r1"}[%W%s]))'
  fi
  _err_q='sum(increase(outbox_publish_total{app="'$_app'",namespace="voltera-r1",result="error"}[%W%s]))'
  _pend_q='max(max_over_time(outbox_pending{app="'$_app'",namespace="voltera-r1"}[%W%s]))'

  # series: pico, t_pico, t_ready, t_cero, valor antes
  # shellcheck disable=SC2046  # la salida son enteros separados por espacios
  set -- $(e1_series "$OUT/raw/series-$_tag.csv" "$_series_col" "$_ready_col" "$_tp" "$_tr")
  _peak=$1 _peakt=$2 _rt=$3 _zt=$4 _td=$5 _vd=$6 _tu=$7 _vu=$8 _lastv=$9
  if [ "$_kind" = transfer ]; then
    _produced_sql="select count(*) from matches where ($_w) and created_at >= to_timestamp(%START%) and created_at < to_timestamp(%END%)"
    _pdb=$E1_NS/matches-db
  else
    _produced_sql="select count(*) from outbox where aggregate_id like '${_prefix}%' and created_at >= to_timestamp(%START%) and created_at < to_timestamp(%END%)"
    _pdb=$E1_NS/positions-db
  fi
  _out_win=$(lx_sql "$_pdb" "$(echo "$_produced_sql" | sed "s/%START%/$_td/; s/%END%/$_tu/")")
  _drain=-1
  [ "$_rt" -ge 0 ] && [ "$_zt" -ge 0 ] && _drain=$((_zt - _rt))

  # emisor: antes (30 s previos a la caída) vs durante (la caída)
  _p95_before=$(e1_prom "$_pq_hist" 30 "$_tp")
  _p95_during=$(e1_prom "$_pq_hist" "$_down" "$_tr")
  _rate_before=$(e1_prom "$_rate_q" 30 "$_tp")
  _rate_during=$(e1_prom "$_rate_q" "$_down" "$_tr")
  _errors=$(e1_prom "$_err_q" $((_ts - _tstart + 30)) "$_ts")
  _pend_before=$(e1_prom "$_pend_q" 30 "$_tp")
  _pend_during=$(e1_prom "$_pend_q" "$_down" "$_tr")
  _pend_all=$(e1_prom "$_pend_q" $((_ts - _tstart + 30)) "$_ts")

  # delta de efectos nuevos en device-sim y billing-sim vs matches creados en la fase
  _created=$(lx_sql "$E1_NS/matches-db" "select count(*) from matches where created_at >= to_timestamp($_tstart) and created_at <= to_timestamp($_ts)")
  _dev1=$(jq -c '{total, distinct, duplicates}' "$OUT/raw/device-$_tag.json")
  _bil1=$(jq -c '.byRegion.r1 | {total, distinct, duplicates}' "$OUT/raw/billing-$_tag.json")

  # vista (solo fase b): keys del prefijo antes de la caída, mínimo durante y después, y al final de la carga
  _keys_before=-1 _keys_min=-1 _keys_end=-1
  if [ "$_kind" = projector ]; then
    # shellcheck disable=SC2046  # la salida son enteros separados por espacios
  set -- $(awk -F, -v tp="$_tp" -v tle="$_tle" "$LX_EPOCH_AWK"'
      NR == 1 { next } $2 == "" { next }
      { t = epoch($1); v = $2 + 0
        if (t <= tp) before = v
        if (t >= tp && t <= tle && (min == "" || v < min)) min = v
        if (t <= tle) last = v }
      END { printf "%d %d %d\n", before, min, last }' "$OUT/raw/keys-$_tag.csv")
    _keys_before=$1 _keys_min=$2 _keys_end=$3
  fi

  jq -n --slurpfile eff "$OUT/raw/effects-$_tag.json" --slurpfile ph "$_ph" \
    --arg kind "$_kind" --argjson peak "$_peak" --argjson drain "$_drain" \
    --argjson outWin "$_out_win" --argjson down "$_down" --argjson vd "$_vd" --argjson vu "$_vu" --argjson lastV "$_lastv" \
    --argjson p95b "$_p95_before" --argjson p95d "$_p95_during" --argjson rb "$_rate_before" --argjson rd "$_rate_during" \
    --argjson errors "$_errors" --argjson pendB "$_pend_before" --argjson pendD "$_pend_during" --argjson pendAll "$_pend_all" \
    --argjson created "$_created" --argjson dev1 "$_dev1" --argjson bil1 "$_bil1" \
    --argjson keysBefore "$_keys_before" --argjson keysMin "$_keys_min" --argjson keysEnd "$_keys_end" '
    $ph[0] as $p
    | {kind: $kind, prefix: $p.prefix, settled: $p.settled,
       matches: $eff[0].matches, device: $eff[0].device, billing: $eff[0].billing,
       created: $created,
       deviceDeltaDistinct: ($dev1.distinct - $p.device0.distinct),
       billingDeltaDistinct: ($bil1.distinct - $p.billing0.distinct),
       deviceDeltaGap: ($created - ($dev1.distinct - $p.device0.distinct)),
       billingDeltaGap: ($created - ($bil1.distinct - $p.billing0.distinct)),
       deviceDeltaDuplicates: ($dev1.duplicates - $p.device0.duplicates),
       downS: $down, producedInOutage: $outWin,
       peak: $peak, valueWhenDown: $vd, valueAtResume: $vu, final: $lastV,
       growth: ($vu - $vd),
       growthRatio: (if $outWin > 0 then (($vu - $vd) / $outWin) else 0 end),
       drainS: $drain,
       emitter: {p95Before: $p95b, p95During: $p95d,
                 p95Ratio: (if $p95b > 0 then ($p95d / $p95b) else 0 end),
                 rateBefore: ($rb / 30), rateDuring: ($rd / $down),
                 rateRatio: (if $rb > 0 then (($rd / $down) / ($rb / 30)) else 0 end),
                 publishErrors: $errors, outboxPendingBefore: $pendB, outboxPendingDuring: $pendD, outboxPendingMax: $pendAll},
       keys: {before: $keysBefore, minDuring: $keysMin, atLoadEnd: $keysEnd,
              lost: (if $keysBefore > 0 then ([0, $keysBefore - $keysMin] | max) else -1 end)}}' >"$OUT/raw/metrics-$_tag.json"
}

collect() {
  _rc=0
  for _t in a b; do
    [ -f "$OUT/raw/phase-$_t.json" ] || { log "falta la fase $_t"; _rc=1; continue; }
    e1_phase_metrics "$_t" || { log "falló la recolección de la fase $_t"; _rc=1; }
  done
  jq -n --slurpfile a "$OUT/raw/metrics-a.json" --slurpfile b "$OUT/raw/metrics-b.json" \
    '{a: $a[0], b: $b[0]}' >"$OUT/metrics.json" || return 1
  # figuras: lag/cola y réplicas con la ventana de caída (columna down = 1)
  for _t in a b; do
    [ -f "$OUT/raw/phase-$_t.json" ] || continue
    _tp=$(jq -r .tPause "$OUT/raw/phase-$_t.json")
    _tr=$(jq -r .tResume "$OUT/raw/phase-$_t.json")
    awk -F, -v tp="$_tp" -v tr="$_tr" "$LX_EPOCH_AWK"'
      NR == 1 { print $0 ",down_x100"; next }
      { t = epoch($1); print $0 "," ((t >= tp && t < tr) ? 100 : 0) }' "$OUT/raw/series-$_t.csv" >"$OUT/figures/fig-$_t.csv"
    if [ "$_t" = a ]; then _y=lag,et,down_x100; else _y=posq,proj,down_x100; fi
    expctl plot --csv "$OUT/figures/fig-$_t.csv" --x ts --y "$_y" --title "E1$_t cola/lag, réplicas y caída (down_x100 = 100 durante la caída)" \
      --out "$OUT/figures/e01$_t-lag-replicas.png" || log "sin figura e01$_t"
  done
  return "$_rc"
}
