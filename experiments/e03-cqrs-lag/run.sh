#!/bin/sh
# E3: CQRS, retraso escritura -> vista. Se carga con `.` desde experiments/run.sh. Región r1.
# Fases: AB (100 decl/s; A sin lecturas concurrentes, B con 200 lecturas/s), C (aislamiento: positions-db a 0 durante 30 s
# con lectores activos), D (pico: spike a SPIKE_RATE desde 0 réplicas). Medición de caja negra con el perfil probe de
# meter-sim: committed (outbox.created_at) -> visible (primera lectura de Redis que ve la versión del sondeo).
# shellcheck disable=SC2154  # OUT, EXPCTL, ROOT, HERE, SPIKE_RATE los define el runner / params.env

# shellcheck source=experiments/lib/probejob.sh
. "$HERE/lib/probejob.sh"
# shellcheck source=experiments/lib/redisview.sh
. "$HERE/lib/redisview.sh"

E3_NS=voltera-r1
E3_REGION=r1
E3_METERS=3000        # 3000 medidores / 30 s = 100 decl/s
E3_PERIOD=30
E3_PROBE_METERS=50
E3_READ_RATE=200
E3_AB_LOAD_S=270      # carga steady que cubre estabilización + sondeo A + pausa + sondeo B
E3_PEAK_S=60
E3_PEAK_PROBE_S=170
E3_CLEANED=0
E3_DB_DOWN=0
E3_JOBS=

e3_cleanup() {
  [ "$E3_CLEANED" -eq 0 ] || return 0
  E3_CLEANED=1
  for j in $E3_JOBS; do k -n "$E3_NS" delete job "$j" --ignore-not-found --wait=false >/dev/null 2>&1 || true; done
  if [ "$E3_DB_DOWN" -eq 1 ]; then
    log "restaura positions-db a 1 réplica"
    k -n "$E3_NS" scale statefulset positions-db --replicas=1 >/dev/null || true
    k -n "$E3_NS" rollout status statefulset/positions-db --timeout=180s >/dev/null || true
  fi
}

# e3_depth <cola>: mensajes ready + unacked en el vhost r1
e3_depth() {
  k -n voltera-infra exec rabbitmq-0 -c rabbitmq -- rabbitmqctl -q list_queues -p "$E3_REGION" name messages_ready messages_unacknowledged 2>/dev/null \
    | awk -v q="$1" '$1 == q { print $2 + $3; found = 1 } END { if (!found) print -1 }'
}

e3_gw_ready() { k -n "$E3_NS" get deploy declaration-gateway -o jsonpath='{.status.readyReplicas}' 2>/dev/null | awk 'END { print $1 + 0 }'; }

# e3_wait_stable <timeout_s>: >= 2 réplicas listas y cola meter-declarations <= 150 en 3 comprobaciones seguidas
e3_wait_stable() {
  _t=0
  _ok=0
  while [ "$_t" -lt "$1" ]; do
    if [ "$(e3_gw_ready)" -ge 2 ] && [ "$(e3_depth meter-declarations)" -le 150 ]; then
      _ok=$((_ok + 1))
    else
      _ok=0
    fi
    [ "$_ok" -ge 3 ] && return 0
    sleep 2
    _t=$((_t + 3))
  done
  return 1
}

# e3_wait_drained <timeout_s>: colas vacías, outbox sin pendientes
e3_wait_drained() {
  _t=0
  while [ "$_t" -lt "$1" ]; do
    _pend=$(expctl sql "$E3_NS/positions-db" "select count(*) from outbox where published_at is null" --no-header 2>/dev/null | tr -d ' \r')
    if [ "$(e3_depth meter-declarations)" = 0 ] && [ "$(e3_depth energy-positions)" = 0 ] && [ "${_pend:-1}" = 0 ]; then
      sleep 3
      return 0
    fi
    sleep 3
    _t=$((_t + 5))
  done
  return 1
}

exp_reset() {
  trap 'e3_cleanup; cluster_unlock; exit 130' INT TERM
  trap 'e3_cleanup; cluster_unlock' EXIT
  need jq
  need awk
  probejob_build || return 1
  k -n "$E3_NS" scale statefulset positions-db --replicas=1 >/dev/null || return 1
  k -n "$E3_NS" rollout status statefulset/positions-db --timeout=180s >/dev/null || return 1
  wait_ready "$E3_NS" market-projector 120 || return 1
  e3_wait_drained 180 || log "el sistema no estaba en reposo al empezar (colas con mensajes)"
}

# e3_converge <etiqueta> <like> <glob>: positions-db == Redis (side, kwh, validUntil, version) para los medidores del
# prefijo; reintenta hasta 90 s. validUntil se compara a resolución de µs (timestamptz de Postgres; el evento lleva ns).
# Los medidores ausentes de Redis cuya fila tiene más de 140 s (TTL 150) se excluyen: expiraron, no divergieron.
# Escribe raw/convergence_<etiqueta>.json
e3_converge() {
  _label=$1
  _t=0
  while :; do
    expctl sql "$E3_NS/positions-db" "select meter_id || '|' || side || '|' || kwh::text || '|' || to_char(valid_until at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') || '|' || version::text || '|' || floor(extract(epoch from now() - updated_at))::text from positions where meter_id like '$2'" --no-header \
      >"$OUT/raw/db_$_label.txt" || return 1
    rv_state "$3" >"$OUT/raw/redis_$_label.txt" || return 1
    res=$(awk -F'|' -v maxage=140 '
      function norm(t) {
        if (t ~ /\./) { sub(/Z$/, "", t); split(t, p, "."); f = substr(p[2] "000000", 1, 6); sub(/0+$/, "", f); t = p[1] (f == "" ? "" : "." f) "Z" }
        return t
      }
      FNR == NR { gsub(/\r/, ""); db[$1] = $2 "|" ($3 + 0) "|" norm($4) "|" $5; age[$1] = $6; next }
      { rd[$1] = $2 "|" ($3 + 0) "|" norm($4) "|" $5 }
      END {
        for (m in db) {
          checked++
          if (!(m in rd)) { if (age[m] > maxage) expired++; else missing++ }
          else if (db[m] == rd[m]) same++
          else { differ++; if (differ <= 5) print "DIFF " m " db=" db[m] " redis=" rd[m] > "/dev/stderr" }
        }
        printf "%d %d %d %d %d\n", checked, same, missing, differ, expired
      }' "$OUT/raw/db_$_label.txt" "$OUT/raw/redis_$_label.txt" 2>>"$OUT/logs/convergence_$_label.log")
    read -r _checked _same _missing _differ _expired <<EOS
$res
EOS
    if { [ "$_missing" -eq 0 ] && [ "$_differ" -eq 0 ]; } || [ "$_t" -ge 90 ]; then
      printf '{"checked":%s,"equal":%s,"missing":%s,"different":%s,"expired_excluded":%s,"waited_s":%s}\n' \
        "$_checked" "$_same" "$_missing" "$_differ" "$_expired" "$_t" >"$OUT/raw/convergence_$_label.json"
      return 0
    fi
    sleep 5
    _t=$((_t + 7))
  done
}

# e3_probe_lines <log> <PREFIJO> <destino>: extrae filas con prefijo del log de un Job
e3_lines() { probejob_lines "$2" <"$1" >"$3"; }

# e3_summary <log>: último resumen JSON de un Job
e3_summary() { grep '^{"profile"' "$1" | tail -n 1; }

drive() {
  RUN=$(date -u +%y%m%d%H%M%S)
  printf '%s\n' "$RUN" >"$OUT/raw/run.txt"
  mkdir -p "$OUT/work"

  # ---------- AB: carga base, sondeo A (sin lecturas) y sondeo B (con 200 lecturas/s) ----------
  log "AB: carga base $E3_METERS medidores / ${E3_PERIOD}s ($((E3_METERS / E3_PERIOD)) decl/s)"
  t_ab_start=$(expctl now)
  probejob_start "$E3_REGION" amqp --profile steady --prefix "e03-$RUN-a-" --meters "$E3_METERS" --period "$E3_PERIOD" \
    --duration "$E3_AB_LOAD_S" --seed 3 || return 1
  steady_ab=$PROBEJOB_LAST
  E3_JOBS="$steady_ab"
  e3_wait_stable 150 || { log "la cola no se estabilizó en 150 s"; return 1; }
  sleep 10

  log "AB: sondeo A (60 s, sin lecturas)"
  tA0=$(expctl now)
  probejob_run "$E3_REGION" probe --profile probe --prefix "probe-e03-$RUN-a-" --meters "$E3_PROBE_METERS" --rate 1 \
    --duration 60 --probe-timeout 60 --out - >"$OUT/logs/probe_a.log" || log "el sondeo A terminó con error"
  tA1=$(expctl now)
  sleep 15

  log "AB: sondeo B (60 s, $E3_READ_RATE lecturas/s)"
  tB0=$(expctl now)
  probejob_run "$E3_REGION" probe --profile probe --prefix "probe-e03-$RUN-b-" --meters "$E3_PROBE_METERS" --rate 1 \
    --duration 60 --probe-timeout 60 --read-rate "$E3_READ_RATE" --out - --read-out - >"$OUT/logs/probe_b.log" || log "el sondeo B terminó con error"
  tB1=$(expctl now)

  probejob_wait "$E3_REGION" "$steady_ab" 200 >"$OUT/logs/steady_ab.log" || log "la carga steady AB terminó con error"
  e3_wait_drained 120 || log "AB: las colas no se vaciaron en 120 s"
  t_ab_end=$(expctl now)
  e3_converge base_steady "e03-$RUN-a-%" "pos:*:e03-$RUN-a-*"
  e3_converge base_probe "probe-e03-$RUN-_-%" "pos:*:probe-e03-$RUN-?-*"

  # ---------- C: aislamiento de lecturas, positions-db a 0 durante 30 s ----------
  log "C: lectores solos; positions-db a 0 réplicas durante 30 s"
  probejob_start "$E3_REGION" probe --profile probe --prefix "probe-e03-$RUN-c-" --rate 0 --read-rate "$E3_READ_RATE" \
    --duration 100 --read-out - || return 1
  readers=$PROBEJOB_LAST
  E3_JOBS="$E3_JOBS $readers"
  sleep 40
  E3_DB_DOWN=1
  k -n "$E3_NS" scale statefulset positions-db --replicas=0 >/dev/null || return 1
  k -n "$E3_NS" wait --for=delete pod/positions-db-0 --timeout=120s >/dev/null || return 1
  tC_down0=$(expctl now)
  sleep 30
  tC_down1=$(expctl now)
  k -n "$E3_NS" scale statefulset positions-db --replicas=1 >/dev/null || return 1
  k -n "$E3_NS" rollout status statefulset/positions-db --timeout=180s >/dev/null || return 1
  E3_DB_DOWN=0
  probejob_wait "$E3_REGION" "$readers" 200 >"$OUT/logs/readers_c.log" || log "los lectores C terminaron con error"

  # ---------- D: pico (spike) desde 0 réplicas ----------
  log "D: espera a que el gateway baje a 0 réplicas"
  _t=0
  while [ "$(k -n "$E3_NS" get deploy declaration-gateway -o jsonpath='{.status.replicas}' | awk 'END { print $1 + 0 }')" -gt 0 ] && [ "$_t" -lt 180 ]; do
    sleep 5
    _t=$((_t + 5))
  done
  log "D: pico de $SPIKE_RATE decl/s durante ${E3_PEAK_S}s y sondeo con $E3_READ_RATE lecturas/s"
  t_d_start=$(expctl now)
  probejob_start "$E3_REGION" amqp --profile spike --prefix "e03-$RUN-p-" --meters "$E3_METERS" --rate "$SPIKE_RATE" \
    --duration "$E3_PEAK_S" --ramp 5 --seed 5 || return 1
  spike=$PROBEJOB_LAST
  probejob_start "$E3_REGION" probe --profile probe --prefix "probe-e03-$RUN-p-" --meters "$E3_PROBE_METERS" --rate 1 \
    --duration "$E3_PEAK_PROBE_S" --probe-timeout 150 --read-rate "$E3_READ_RATE" --out - --read-out - || return 1
  probe_d=$PROBEJOB_LAST
  E3_JOBS="$E3_JOBS $spike $probe_d"
  probejob_wait "$E3_REGION" "$spike" 300 >"$OUT/logs/spike.log" || log "el pico terminó con error"
  # la convergencia de los medidores del pico se comprueba en cuanto se vacían las colas (TTL de 150 s), con el sondeo aún activo
  e3_wait_drained 240 || log "D: las colas no se vaciaron en 240 s"
  t_d_end=$(expctl now)
  e3_converge peak_steady "e03-$RUN-p-%" "pos:*:e03-$RUN-p-*"
  probejob_wait "$E3_REGION" "$probe_d" 400 >"$OUT/logs/probe_p.log" || log "el sondeo del pico terminó con error"
  e3_converge peak_probe "probe-e03-$RUN-p-%" "pos:*:probe-e03-$RUN-p-*"
  sleep 20 # un scrape de Prometheus antes de collect
}

# ---------------- collect ----------------

# e3_prom <consulta con @RANGE@> <t_ini> <t_fin>: valor numérico o null; @RANGE@ -> [<dur>s] offset <desde_fin>s
e3_prom() {
  _w=$(expctl elapsed "$2" "$3" | awk '{ printf "%d", $1 + 0.5 }')
  _o=$(expctl elapsed "$3" "$(expctl now)" | awk '{ printf "%d", ($1 > 0 ? $1 : 0) }')
  _q=$(printf '%s' "$1" | sed "s/@RANGE@/[${_w}s] offset ${_o}s/g")
  expctl prom query "$_q" 2>/dev/null \
    | jq -r 'if (.data.result | length) == 0 then "null" else (.data.result[0].value[1] | if . == "NaN" or . == "+Inf" or . == "-Inf" then "null" else . end) end' 2>/dev/null \
    || echo null
}

# e3_lag_json <t_ini> <t_fin> <etapa>: {p50,p95,p99,mean} de event_e2e_lag_seconds{market-projector, etapa}
e3_lag_json() {
  _sel="service=\"market-projector\",stage=\"$3\""
  _b="sum by (le)(increase(event_e2e_lag_seconds_bucket{$_sel}@RANGE@))"
  jq -n --argjson p50 "$(e3_prom "histogram_quantile(0.5, $_b)" "$1" "$2")" \
    --argjson p95 "$(e3_prom "histogram_quantile(0.95, $_b)" "$1" "$2")" \
    --argjson p99 "$(e3_prom "histogram_quantile(0.99, $_b)" "$1" "$2")" \
    --argjson mean "$(e3_prom "sum(increase(event_e2e_lag_seconds_sum{$_sel}@RANGE@)) / sum(increase(event_e2e_lag_seconds_count{$_sel}@RANGE@))" "$1" "$2")" \
    '{p50: $p50, p95: $p95, p99: $p99, mean: $mean}'
}

# e3_decomp <t_ini> <t_fin>: descomposición por medias; committed->emitted = media(committed) - media(emitted);
# emitted->consumido = media(emitted) - media(procesamiento); consumido->SET = media(event_processing_seconds del proyector)
e3_decomp() {
  _c=$(e3_lag_json "$1" "$2" committed)
  _e=$(e3_lag_json "$1" "$2" emitted)
  _pm=$(e3_prom 'sum(increase(event_processing_seconds_sum{service="market-projector",event_type="PositionDeclared"}@RANGE@)) / sum(increase(event_processing_seconds_count{service="market-projector",event_type="PositionDeclared"}@RANGE@))' "$1" "$2")
  jq -n --argjson c "$_c" --argjson e "$_e" --argjson pm "$_pm" \
    '{committed_to_visible: $c, emitted_to_visible: $e, consumed_to_set_mean_s: $pm,
      committed_to_emitted_mean_s: (if $c.mean != null and $e.mean != null then ($c.mean - $e.mean) else null end),
      emitted_to_consumed_mean_s: (if $e.mean != null and $pm != null then ($e.mean - $pm) else null end)}'
}

# e3_write_p95 <t_ini> <t_fin>: p95 (s) de event_processing_seconds del gateway en la ventana
e3_write_p95() {
  e3_prom 'histogram_quantile(0.95, sum by (le)(increase(event_processing_seconds_bucket{service="declaration-gateway"}@RANGE@)))' "$1" "$2"
}

# e3_join <probe.csv> <outbox.csv> <timeout_s> <salida>: ts,sent_s,meterId,status,committed_to_visible_s,sent_to_visible_s
e3_join() {
  awk -F, -v to="$3" '
    FNR == NR { if (FNR > 1) c[$1] = $2; next }
    FNR == 1 { print "ts,sent_s,meterId,status,committed_to_visible_s,sent_to_visible_s"; next }
    {
      if ($9 == "ok" && ($1 in c)) { lat = ($8 - c[$1]) / 1e6; s2v = ($8 - $7) / 1e6 } else { lat = to; s2v = to }
      printf "%s,%.3f,%s,%s,%.6f,%.6f\n", $4, $7 / 1e6, $2, $9, lat, s2v
    }' "$2" "$1" >"$4"
}

# e3_cut <join.csv> <desde_s> <hasta_s> <salida>: filas con desde <= sent_s < hasta, sin la columna sent_s
e3_cut() {
  awk -F, -v lo="$2" -v hi="$3" 'NR == 1 { print "ts,meterId,status,committed_to_visible_s,sent_to_visible_s"; next }
    $2 >= lo && $2 < hi { print $1 "," $3 "," $4 "," $5 "," $6 }' "$1" >"$4"
}

# e3_quant <csv> <col>: {n,p50,p95,p99,max}
e3_quant() {
  _n=$(awk 'END { print NR - 1 }' "$1")
  if [ "$_n" -le 0 ]; then
    echo '{"n":0,"p50":null,"p95":null,"p99":null,"max":null}'
    return 0
  fi
  _q=$(expctl quantile --file "$1" --col "$2" --q 0.5,0.95,0.99) || { echo '{"n":0,"p50":null,"p95":null,"p99":null,"max":null}'; return 0; }
  _mx=$(awk -F, -v c="$2" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == c) col = i; next } { if ($col + 0 > m) m = $col + 0 } END { print m + 0 }' "$1")
  printf '%s' "$_q" | jq -c --argjson max "$_mx" '{n: .n, p50: .quantiles["0.5"], p95: .quantiles["0.95"], p99: .quantiles["0.99"], max: $max}'
}

# e3_cdf <csv> <col> <salida>: x ordenado y fracción acumulada
e3_cdf() {
  awk -F, -v c="$2" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == c) col = i; next } { print $col + 0 }' "$1" | sort -n \
    | awk 'BEGIN { print "latency_s,fraction" } { v[NR] = $1 } END { for (i = 1; i <= NR; i++) printf "%.6f,%.4f\n", v[i], i / NR }' >"$3"
}

# e3_window <reads.csv> <desde> <hasta> <salida>: filas de lecturas con desde <= ts < hasta (a segundos)
e3_window() {
  _lo=$(printf '%s' "$2" | cut -c1-19)
  _hi=$(printf '%s' "$3" | cut -c1-19)
  awk -F, -v lo="$_lo" -v hi="$_hi" 'NR == 1 { print; next } { t = substr($1, 1, 19); if (t >= lo && t < hi) print }' "$1" >"$4"
}

# e3_reads_stats <window.csv>: {seconds, reads, errors, p95_median_ms}
e3_reads_stats() {
  _s=$(awk -F, 'NR > 1 { n++; r += $2; e += $3 } END { printf "{\"seconds\":%d,\"reads\":%d,\"errors\":%d}", n, r, e }' "$1")
  _m=$(expctl quantile --file "$1" --col p95_ms --q 0.5 2>/dev/null | jq '.quantiles["0.5"]' 2>/dev/null || echo null)
  printf '%s' "$_s" | jq -c --argjson m "${_m:-null}" '. + {p95_median_ms: $m}'
}

collect() {
  W=$OUT/work
  expctl sql "$E3_NS/positions-db" "select event_id, (extract(epoch from created_at) * 1000000)::bigint as committed_us from outbox where aggregate_id like 'probe-e03-$RUN-%'" \
    >"$W/outbox_probe.csv" || return 1

  for ph in a b p; do e3_lines "$OUT/logs/probe_$ph.log" PROBE "$W/probe_$ph.csv"; done
  e3_lines "$OUT/logs/probe_b.log" READS "$OUT/raw/reads_b.csv"
  e3_lines "$OUT/logs/probe_p.log" READS "$OUT/raw/reads_peak.csv"
  e3_lines "$OUT/logs/readers_c.log" READS "$OUT/raw/reads_c.csv"
  mkdir -p "$OUT/data"
  cp "$W/outbox_probe.csv" "$W/probe_a.csv" "$W/probe_b.csv" "$W/probe_p.csv" "$OUT/data/"

  e3_join "$W/probe_a.csv" "$W/outbox_probe.csv" 60 "$W/join_a.csv"
  e3_join "$W/probe_b.csv" "$W/outbox_probe.csv" 60 "$W/join_b.csv"
  e3_join "$W/probe_p.csv" "$W/outbox_probe.csv" 150 "$W/join_p.csv"
  e3_cut "$W/join_a.csv" 0 99999999999 "$OUT/raw/latency_base_noread.csv"
  e3_cut "$W/join_b.csv" 0 99999999999 "$OUT/raw/latency_base_read.csv"

  # pico: durante = sondeos enviados entre el inicio y el fin de la publicación del spike; recuperación = enviados 60 s después
  sp=$(e3_summary "$OUT/logs/spike.log")
  sp_start=$(printf '%s' "$sp" | jq -r '.startedAt | sub("\\.[0-9]+Z$"; "Z") | fromdate')
  sp_end=$(printf '%s' "$sp" | jq -r '.endedAt | sub("\\.[0-9]+Z$"; "Z") | fromdate')
  e3_cut "$W/join_p.csv" "$sp_start" "$sp_end" "$OUT/raw/latency_peak_during.csv"
  e3_cut "$W/join_p.csv" $((sp_end + 60)) 99999999999 "$OUT/raw/latency_peak_recovered.csv"
  e3_cut "$W/join_p.csv" 0 99999999999 "$OUT/raw/latency_peak_all.csv"
  # tiempo hasta volver a <= 5 s: último sondeo enviado después del pico con retraso > 5 s. recovery_s usa committed->visible
  # (la métrica de E3); e2e_recovery_s usa enviado->visible (incluye la espera en la cola meter-declarations, que es de E2)
  recovery_s=$(awk -F, -v end="$sp_end" 'NR > 1 && $2 >= end && $5 + 0 > 5 { if ($2 > last) last = $2 } END { print (last ? last - end : 0) }' "$W/join_p.csv")
  e2e_recovery_s=$(awk -F, -v end="$sp_end" 'NR > 1 && $2 >= end && $6 + 0 > 5 { if ($2 > last) last = $2 } END { print (last ? last - end : 0) }' "$W/join_p.csv")

  mkdir -p "$OUT/cdf"
  e3_cdf "$OUT/raw/latency_base_noread.csv" committed_to_visible_s "$OUT/cdf/cdf_base_noread.csv"
  e3_cdf "$OUT/raw/latency_base_read.csv" committed_to_visible_s "$OUT/cdf/cdf_base_read.csv"
  e3_cdf "$OUT/raw/latency_peak_all.csv" committed_to_visible_s "$OUT/cdf/cdf_peak.csv"
  for c in base_noread base_read peak; do
    expctl plot --csv "$OUT/cdf/cdf_$c.csv" --x latency_s --y fraction --title "E3 CDF committed->visible ($c)" \
      --out "$OUT/figures/cdf_$c.png" || log "sin CDF de $c"
  done

  # lecturas: aislamiento de positions-db
  _c0=$(head -n 2 "$OUT/raw/reads_c.csv" | tail -n 1 | cut -d, -f1)
  e3_window "$OUT/raw/reads_c.csv" "$_c0" "$tC_down0" "$W/reads_c_up.csv"
  e3_window "$OUT/raw/reads_c.csv" "$tC_down0" "$tC_down1" "$W/reads_c_down.csv"
  # se descartan los 2 primeros segundos de cada ventana (arranque del lector / corte de la base)
  for f in up down; do awk -F, 'NR == 1 || NR > 3' "$W/reads_c_$f.csv" >"$W/reads_c_${f}_trim.csv"; done
  up=$(e3_reads_stats "$W/reads_c_up_trim.csv")
  down=$(e3_reads_stats "$W/reads_c_down_trim.csv")
  all_c=$(e3_reads_stats "$OUT/raw/reads_c.csv")
  cp "$W/reads_c_down.csv" "$OUT/raw/reads_c_positions_db_down.csv"

  # escritura: p95 del procesamiento del gateway con y sin lectura concurrente
  wp_a=$(e3_write_p95 "$tA0" "$tA1")
  wp_b=$(e3_write_p95 "$tB0" "$tB1")

  decomp_ab=$(e3_decomp "$t_ab_start" "$t_ab_end")
  decomp_d=$(e3_decomp "$t_d_start" "$t_d_end")
  # la consulta del enunciado, sobre toda la corrida
  committed_p99_all=$(e3_prom 'histogram_quantile(0.99, sum by (le)(increase(event_e2e_lag_seconds_bucket{service="market-projector",stage="committed"}@RANGE@)))' "$t_ab_start" "$t_d_end")

  late_ab=$(e3_summary "$OUT/logs/steady_ab.log" | jq -c '{published, late, dropped, errors}' 2>/dev/null || echo '{}')
  late_sp=$(printf '%s' "$sp" | jq -c '{published, late, dropped, errors}' 2>/dev/null || echo '{}')
  reads_b=$(e3_summary "$OUT/logs/probe_b.log" | jq -c '.reads // {}' 2>/dev/null || echo '{}')
  reads_p=$(e3_summary "$OUT/logs/probe_p.log" | jq -c '.reads // {}' 2>/dev/null || echo '{}')
  sum_a=$(e3_summary "$OUT/logs/probe_a.log" | jq -c '{sent, visible, timeouts, errors}' 2>/dev/null || echo '{}')
  sum_b=$(e3_summary "$OUT/logs/probe_b.log" | jq -c '{sent, visible, timeouts, errors}' 2>/dev/null || echo '{}')
  sum_p=$(e3_summary "$OUT/logs/probe_p.log" | jq -c '{sent, visible, timeouts, errors}' 2>/dev/null || echo '{}')

  jq -n \
    --arg run "$RUN" \
    --argjson qa "$(e3_quant "$OUT/raw/latency_base_noread.csv" committed_to_visible_s)" \
    --argjson qb "$(e3_quant "$OUT/raw/latency_base_read.csv" committed_to_visible_s)" \
    --argjson qd "$(e3_quant "$OUT/raw/latency_peak_during.csv" committed_to_visible_s)" \
    --argjson qr "$(e3_quant "$OUT/raw/latency_peak_recovered.csv" committed_to_visible_s)" \
    --argjson qall "$(e3_quant "$OUT/raw/latency_peak_all.csv" committed_to_visible_s)" \
    --argjson recovery "$recovery_s" --argjson e2e_recovery "$e2e_recovery_s" \
    --argjson e2e_a "$(e3_quant "$OUT/raw/latency_base_noread.csv" sent_to_visible_s)" \
    --argjson e2e_b "$(e3_quant "$OUT/raw/latency_base_read.csv" sent_to_visible_s)" \
    --argjson e2e_d "$(e3_quant "$OUT/raw/latency_peak_during.csv" sent_to_visible_s)" \
    --argjson e2e_all "$(e3_quant "$OUT/raw/latency_peak_all.csv" sent_to_visible_s)" \
    --argjson sum_a "$sum_a" --argjson sum_b "$sum_b" --argjson sum_p "$sum_p" \
    --argjson up "$up" --argjson down "$down" --argjson all_c "$all_c" \
    --argjson reads_b "$reads_b" --argjson reads_p "$reads_p" \
    --argjson wp_a "$wp_a" --argjson wp_b "$wp_b" \
    --argjson decomp_ab "$decomp_ab" --argjson decomp_d "$decomp_d" --argjson cp99 "$committed_p99_all" \
    --argjson load_ab "$late_ab" --argjson load_sp "$late_sp" \
    --slurpfile cb1 "$OUT/raw/convergence_base_steady.json" --slurpfile cb2 "$OUT/raw/convergence_base_probe.json" \
    --slurpfile cp1 "$OUT/raw/convergence_peak_steady.json" --slurpfile cp2 "$OUT/raw/convergence_peak_probe.json" \
    '{run: $run,
      base_noread: ($qa + {probes: $sum_a}),
      base_read: ($qb + {probes: $sum_b, reads: $reads_b}),
      peak: {during: $qd, recovered: $qr, all: ($qall + {probes: $sum_p, reads: $reads_p}), recovery_s: $recovery},
      sent_to_visible: {note: "incluye la espera en la cola meter-declarations antes del commit (tema de E2)",
        base_noread: $e2e_a, base_read: $e2e_b, peak_during: $e2e_d, peak_all: $e2e_all, peak_recovery_s: $e2e_recovery},
      isolation: {baseline: $up, positions_db_down: $down, reader_job_total: $all_c,
        p95_change_pct_abs: (if ($up.p95_median_ms // 0) > 0 and $down.p95_median_ms != null
                             then (((($down.p95_median_ms - $up.p95_median_ms) | fabs)) as $d
                                   | if $d <= 0.5 then 0 else ($d / $up.p95_median_ms * 100) end) else null end)},
      write: {p95_noread_s: $wp_a, p95_read_s: $wp_b,
        increase_pct: (if $wp_a != null and $wp_b != null and $wp_a > 0 then (($wp_b - $wp_a) / $wp_a * 100) else null end),
        increase_pct_effective: (if $wp_a != null and $wp_b != null and $wp_a > 0
                                 then (if ($wp_b - $wp_a) <= 0.005 then 0 else (($wp_b - $wp_a) / $wp_a * 100) end) else null end)},
      decomposition: {base: $decomp_ab, peak: $decomp_d, committed_p99_whole_run_s: $cp99},
      load: {base: $load_ab, peak: $load_sp},
      convergence: {base_steady: $cb1[0], base_probe: $cb2[0], peak_steady: $cp1[0], peak_probe: $cp2[0]}}' >"$OUT/metrics.json"
  jq '.decomposition' "$OUT/metrics.json" >"$OUT/decomposition.json"
  jq '{base_noread, base_read, peak}' "$OUT/metrics.json" >"$OUT/latency_quantiles.json"
  jq '.convergence' "$OUT/metrics.json" >"$OUT/convergence.json"
}
