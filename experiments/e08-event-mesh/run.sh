#!/bin/sh
# E8: event mesh multi-región con autorización. Se carga con `.` desde experiments/run.sh (define drive y collect).
# Fases de drive: normal (120 s, r1 ~10 pares/s y r2 ~3 pares/s) -> negativos (4 casos x 20 intentos) -> partición r2<->global.
# shellcheck shell=sh disable=SC2329,SC2034

# shellcheck source=experiments/lib/metersim.sh
. "$HERE/lib/metersim.sh"
# shellcheck source=experiments/lib/mesh.sh
. "$HERE/lib/mesh.sh"

E08_RUN=$(date -u +%d%H%M%S)          # prefijo único por corrida: nada se mezcla con tráfico previo
E08_PFX=e8-$E08_RUN
E08_LOAD_S=${E08_LOAD_S:-120}
E08_PAIRS_R1=${E08_PAIRS_R1:-20}      # --rate en declaraciones/s = 2 x pares/s (10 pares/s)
E08_PAIRS_R2=${E08_PAIRS_R2:-6}       # 3 pares/s
E08_CUT_R1=${E08_CUT_R1:-4}           # carga ligera de r1 durante el corte (r1 no debe notarlo)
E08_DRAIN_LIMIT_S=${E08_DRAIN_LIMIT_S:-120}
E08_LOCAL_MARGIN_S=${E08_LOCAL_MARGIN_S:-30}  # un match creado en los últimos 30 s del corte puede seguir en el pipeline de transferencia
E08_STOP=$OUT/raw/.stop-sampler
E08_BG=

# prefijo de medidores de una fase: $E08_PFX-<fase>-<región>-
e08_prefix() { printf '%s-%s-' "$E08_PFX" "$1"; }

# e08_spec <fase> <región>: especificación tag=región=prefijo para mesh_settle
e08_spec() { printf '%s-%s=%s=%s' "$1" "$2" "$2" "$(e08_prefix "$1-$2")"; }

# ---- reset / warmup / limpieza ----

e08_cleanup() {
  : >"$E08_STOP" 2>/dev/null || true
  for _p in $E08_BG; do kill "$_p" 2>/dev/null || true; done
  chaos_heal >/dev/null 2>&1 || true
  for _n in neg-rogue neg-nocred neg-bridge-r2 neg-sub; do
    mesh_probe_cleanup voltera-r1 "$_n"
    mesh_probe_cleanup voltera-r2 "$_n"
  done
  k -n voltera-r1 delete sa zt-stranger --ignore-not-found >/dev/null 2>&1 || true
  # nats-r2 debe quedar sano y con el enlace restablecido
  k -n voltera-r2 rollout status deploy/nats --timeout=90s >/dev/null 2>&1 || true
  [ "${E08_KEEP_TOOLS:-0}" = 1 ] || mesh_tools_down
}
trap 'e08_cleanup; cluster_unlock' EXIT
trap 'exit 130' INT TERM

e08_links_ok() {
  [ "$(mesh_leafs global 2>/dev/null)" = 2 ] && [ "$(mesh_leafs r1 2>/dev/null)" = 1 ] && [ "$(mesh_leafs r2 2>/dev/null)" = 1 ]
}

exp_reset() {
  chaos_heal
  if [ "${SKIP_BUILD:-0}" != 1 ]; then
    MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/images/build" billing-sim meter-sim zt-probe >"$OUT/logs/images-build.log" 2>&1 \
      || { cat "$OUT/logs/images-build.log" >&2; return 1; }
  fi
  mesh_tools_up || return 1
  # billing-sim con la imagen actual (latencias por match). Su libro vive en memoria: el reinicio lo deja en cero y el
  # durable conserva su posición en el stream, así que no se reentrega nada.
  k -n voltera-global rollout restart deploy/billing-sim >/dev/null || return 1
  mesh_wait_deploy voltera-global billing-sim || return 1
  for _d in voltera-r1/mesh-bridge voltera-r2/mesh-bridge voltera-r1/matching-engine voltera-r2/matching-engine \
    voltera-r1/market-projector voltera-r2/market-projector voltera-r1/device-sim voltera-r2/device-sim; do
    mesh_wait_deploy "${_d%%/*}" "${_d#*/}" 120 || return 1
  done
  mesh_retry 90 e08_links_ok || { log "los leafnodes no están en 2/1/1"; return 1; }
  mesh_sa_up voltera-r1 zt-stranger
  mkdir -p "$OUT/raw"
}

exp_warmup() {
  # arranca KEDA (gateway, energy-transfer) y calienta conexiones: 20 s de pares en ambas regiones
  _w1=$(e08_prefix w-r1)
  _w2=$(e08_prefix w-r2)
  mesh_pairs r1 "$_w1" 10 20 "$OUT/raw/meter-w-r1.log" &
  _wp1=$!
  mesh_pairs r2 "$_w2" 4 20 "$OUT/raw/meter-w-r2.log" &
  _wp2=$!
  wait "$_wp1" || return 1
  wait "$_wp2" || return 1
  mesh_settle 120 "$(e08_spec w r1)" "$(e08_spec w r2)"
}

# ---- drive ----

e08_phase_normal() {
  log "E8 normal: r1 ${E08_PAIRS_R1}/s y r2 ${E08_PAIRS_R2}/s declaraciones durante ${E08_LOAD_S}s"
  E08_REJ0=$(( $(mesh_counter voltera-r1 mesh-bridge mesh_publish_rejected_total) + $(mesh_counter voltera-r2 mesh-bridge mesh_publish_rejected_total) ))
  E08_T_N0=$(expctl now)
  mesh_pairs r1 "$(e08_prefix n-r1)" "$E08_PAIRS_R1" "$E08_LOAD_S" "$OUT/raw/meter-n-r1.log" &
  _p1=$!
  mesh_pairs r2 "$(e08_prefix n-r2)" "$E08_PAIRS_R2" "$E08_LOAD_S" "$OUT/raw/meter-n-r2.log" &
  _p2=$!
  wait "$_p1" || return 1
  wait "$_p2" || return 1
  E08_T_N1=$(expctl now)
  mesh_settle "$E08_DRAIN_LIMIT_S" "$(e08_spec n r1)" "$(e08_spec n r2)" || return 1
  E08_T_N2=$(expctl now)
  E08_REJ1=$(( $(mesh_counter voltera-r1 mesh-bridge mesh_publish_rejected_total) + $(mesh_counter voltera-r2 mesh-bridge mesh_publish_rejected_total) ))
  cp "$OUT/raw/ledger.json" "$OUT/raw/ledger-after-normal.json"
}

# e08_neg_cells <archivo> <caso> <operación> <subject>: 20 celdas iguales (intentos)
e08_neg_cells() {
  _i=1
  : >"$1"
  while [ "$_i" -le 20 ]; do
    printf 'neg-%s-%02d,nats,%s,%s\n' "$2" "$_i" "$3" "$4" >>"$1"
    _i=$((_i + 1))
  done
}

e08_phase_negatives() {
  log "E8 negativos: 4 casos x 20 intentos"
  mkdir -p "$OUT/raw/cells"
  E08_SEQ0=$(mesh_stream_seq)
  mesh_ledger "$OUT/raw/ledger-before-neg.json" || return 1
  E08_LED0=$(jq '.total' "$OUT/raw/ledger-before-neg.json")
  _subj="energy.matched.r1.zt-$E08_RUN"
  e08_neg_cells "$OUT/raw/cells/cells-rogue.csv" rogue publish "$_subj"
  e08_neg_cells "$OUT/raw/cells/cells-nocred.csv" nocred publish "$_subj"
  e08_neg_cells "$OUT/raw/cells/cells-bridge-r2.csv" bridge-r2 publish "$_subj"
  e08_neg_cells "$OUT/raw/cells/cells-sub.csv" sub subscribe 'energy.matched.>'
  # rogue y sin credencial desde un SA ajeno de r1; bridge-r2 comprometido (SA mesh-bridge de r2) publicando en un subject de r1;
  # energy-transfer de r2 (con la única credencial NATS de su namespace) intentando suscribir energy.matched.>
  mesh_probe_job voltera-r1 neg-rogue zt-stranger "$OUT/raw/cells/cells-rogue.csv" \
    NATS_URL=nats://nats.voltera-r1:4222 NATS_USER=rogue NATS_PASSWORD=dev-rogue-77d2 || return 1
  mesh_probe_job voltera-r1 neg-nocred zt-stranger "$OUT/raw/cells/cells-nocred.csv" NATS_URL=nats://nats.voltera-r1:4222 || return 1
  mesh_probe_job voltera-r2 neg-bridge-r2 mesh-bridge "$OUT/raw/cells/cells-bridge-r2.csv" \
    NATS_URL=nats://nats.voltera-r2:4222 NATS_USER=secret:mesh-bridge-credentials:NATS_USER \
    NATS_PASSWORD=secret:mesh-bridge-credentials:NATS_PASSWORD || return 1
  mesh_probe_job voltera-r2 neg-sub energy-transfer "$OUT/raw/cells/cells-sub.csv" \
    NATS_URL=nats://nats.voltera-r2:4222 NATS_USER=secret:mesh-bridge-credentials:NATS_USER \
    NATS_PASSWORD=secret:mesh-bridge-credentials:NATS_PASSWORD || return 1
  mesh_probe_wait voltera-r1 neg-rogue "$OUT/raw/authz-rogue.jsonl" || return 1
  mesh_probe_wait voltera-r1 neg-nocred "$OUT/raw/authz-nocred.jsonl" || return 1
  mesh_probe_wait voltera-r2 neg-bridge-r2 "$OUT/raw/authz-bridge-r2.jsonl" || return 1
  mesh_probe_wait voltera-r2 neg-sub "$OUT/raw/authz-sub.jsonl" || return 1
  sleep 3
  # los logs de nats-server de r2 se pierden al reiniciar el nodo en la partición: se guardan ahora
  for _n in r1 r2 global; do
    k -n "voltera-$_n" logs deploy/nats -c nats --since=10m >"$OUT/logs/nats-server-negatives-$_n.log" 2>&1 || true
  done
  grep -h -i "violation" "$OUT"/logs/nats-server-negatives-*.log >"$OUT/nats_server_violations.txt" || true
  E08_SEQ1=$(mesh_stream_seq)
  mesh_ledger "$OUT/raw/ledger-after-neg.json" || return 1
  E08_LED1=$(jq '.total' "$OUT/raw/ledger-after-neg.json")
}

# e08_sampler <archivo> <stopfile>: una muestra por segundo de leafz (global y r2) y del total de device-sim r2
e08_sampler() {
  while [ ! -e "$2" ]; do
    _ts=$(expctl now)
    k -n "$MESH_TOOLS_NS" exec -i deploy/toolbox -c toolbox -- sh -s 2>/dev/null <<'EOS' | jq -c --arg ts "$_ts" '{ts: $ts, global_leafs: (.g.leafnodes // null), global_names: ([.g.leafs[]?.name]), r2_leafs: (.r.leafnodes // null), r2_remote: ([.r.leafs[]? | {name, is_spoke, rtt}]), device_total: (.d.total // null), device_distinct: (.d.distinct // null)}' >>"$1" 2>/dev/null || true
g=$(curl -fsS -m 3 http://nats.voltera-global:8222/leafz 2>/dev/null || echo null)
r=$(curl -fsS -m 3 http://nats.voltera-r2:8222/leafz 2>/dev/null || echo null)
d=$(curl -fsS -m 3 http://device-sim.voltera-r2:8080/state 2>/dev/null | jq -c '{total, distinct}' 2>/dev/null || echo null)
printf '{"g":%s,"r":%s,"d":%s}\n' "$g" "$r" "$d"
EOS
    sleep 1
  done
}

e08_phase_partition() {
  log "E8 partición: r2 <-> global durante ${E08_LOAD_S}s con carga en r2"
  rm -f "$E08_STOP"
  : >"$OUT/raw/timeline.jsonl"
  e08_sampler "$OUT/raw/timeline.jsonl" "$E08_STOP" &
  E08_BG="$E08_BG $!"
  expctl sample --interval 5s --duration "$((E08_LOAD_S + E08_DRAIN_LIMIT_S + 90))s" --out "$OUT/raw/lag.csv" \
    --prom lag_r2='sum(kafka_consumergroup_lag_sum{consumergroup="mesh-bridge",topic="r2.energy-matched"})' \
    --prom lag_r1='sum(kafka_consumergroup_lag_sum{consumergroup="mesh-bridge",topic="r1.energy-matched"})' \
    --replicas nats_r2=voltera-r2/nats >/dev/null 2>"$OUT/logs/sample-lag.log" &
  E08_BG="$E08_BG $!"
  sleep 8
  mesh_monitor global leafz >"$OUT/raw/leafz-global-before.json" || true
  mesh_monitor r2 leafz >"$OUT/raw/leafz-r2-before.json" || true
  E08_DEV_BEFORE=$(tb curl -fsS --max-time 20 http://device-sim.voltera-r2:8080/state | jq -c '{total, distinct}') || return 1
  # DENY L4 al 7422 del hub y reinicio de nats-r2 para tirar el enlace ya establecido
  E08_T_CUT=$(expctl now)
  chaos_partition voltera-r2 voltera-global 7422 || return 1
  chaos_kill voltera-r2 nats || return 1
  mesh_retry 90 e08_global_down || log "aviso: global sigue viendo 2 leafnodes"
  E08_T_DOWN=$(expctl now)
  mesh_wait_deploy voltera-r2 nats 120 || return 1
  sleep 6   # margen para que el leaf de r2 intente (y falle) reconectar
  mesh_monitor global leafz >"$OUT/raw/leafz-global-partition.json" || true
  mesh_monitor r2 leafz >"$OUT/raw/leafz-r2-partition.json" || true
  mesh_pairs r2 "$(e08_prefix p-r2)" "$E08_PAIRS_R2" "$E08_LOAD_S" "$OUT/raw/meter-p-r2.log" &
  _p2=$!
  mesh_pairs r1 "$(e08_prefix p-r1)" "$E08_CUT_R1" "$E08_LOAD_S" "$OUT/raw/meter-p-r1.log" &
  _p1=$!
  wait "$_p2" || return 1
  wait "$_p1" || return 1
  tb curl -fsS --max-time 30 http://device-sim.voltera-r2:8080/state >"$OUT/raw/device-r2-at-heal.json" || return 1
  E08_T_HEAL=$(expctl now)
  chaos_heal || return 1
  mesh_retry 90 e08_links_ok || log "aviso: los leafnodes no volvieron a 2/1/1"
  E08_T_LINK=$(expctl now)
  mesh_settle "$E08_DRAIN_LIMIT_S" "$(e08_spec p r2)" "$(e08_spec p r1)" || return 1
  E08_T_DRAINED=$(expctl now)
  mesh_monitor global leafz >"$OUT/raw/leafz-global-healed.json" || true
  mesh_monitor r2 leafz >"$OUT/raw/leafz-r2-healed.json" || true
  # el device de r2 termina de recibir los últimos comandos
  sleep 20
  tb curl -fsS --max-time 30 http://device-sim.voltera-r2:8080/state >"$OUT/raw/device-r2-final.json" || return 1
  : >"$E08_STOP"
}

e08_global_down() { [ "$(mesh_leafs global 2>/dev/null)" = 1 ]; }

drive() {
  e08_phase_normal || return 1
  e08_phase_negatives || return 1
  e08_phase_partition || return 1
}

# ---- collect ----

# e08_tag_stats <fase-región> <ledger>: JSON con entrega y percentiles de latencia de esa fase
e08_tag_stats() {
  jq -n --slurpfile L "$2" --rawfile M "$OUT/raw/matches-$1.psv" '
    def pct($a; $q): ($a | sort) as $s
      | if ($s | length) == 0 then null
        else ($q * (($s | length) - 1)) as $p
          | $s[$p | floor] + ($s[$p | ceil] - $s[$p | floor]) * ($p - ($p | floor)) end;
    ($L[0].byMatchId) as $b
    | ($M | split("\n") | map(select(length > 0) | split("|")[0])) as $ids
    | ($ids | map(select(($b[.].count // 0) >= 1))) as $got
    | ($got | map($b[.].emitLagMs / 1000)) as $emit
    | ($got | map($b[.].e2eLagMs / 1000)) as $e2e
    | {matches: ($ids | length), delivered: ($got | length), missing: (($ids | length) - ($got | length)),
       duplicates: ($ids | map(select(($b[.].count // 0) > 1)) | length),
       emit_p50_s: pct($emit; 0.5), emit_p95_s: pct($emit; 0.95), emit_p99_s: pct($emit; 0.99), emit_max_s: ($emit | max),
       e2e_p50_s: pct($e2e; 0.5), e2e_p95_s: pct($e2e; 0.95), e2e_p99_s: pct($e2e; 0.99), e2e_max_s: ($e2e | max),
       last_received_at: ($got | map($b[.].receivedAt) | max)}'
}

e08_latency_rows() { # fase-región región
  jq -r --rawfile M "$OUT/raw/matches-$1.psv" --arg phase "${1%-*}" --arg region "$2" '
    . as $r | ($M | split("\n") | map(select(length > 0) | split("|")[0]))[]
    | select(($r.byMatchId[.].count // 0) >= 1)
    | [$r.byMatchId[.].receivedAt, $phase, $region, ., ($r.byMatchId[.].emitLagMs / 1000), ($r.byMatchId[.].e2eLagMs / 1000), $r.byMatchId[.].count]
    | @csv' "$OUT/raw/ledger.json"
}

e08_negatives_json() {
  jq -n '[inputs | . + {case: (input_filename | sub("^.*/authz-"; "") | sub("\\.jsonl$"; ""))}]' "$OUT"/raw/authz-*.jsonl >"$OUT/raw/authz.json"
  jq -r '["case", "attempt", "operation", "target", "outcome", "class", "error"],
    (.[] | [.case, .id, .op, .target, .outcome, (.class // ""), (.error // "")]) | @csv' "$OUT/raw/authz.json" >"$OUT/authz_attempts.csv"
  jq --argjson seq0 "${E08_SEQ0:-0}" --argjson seq1 "${E08_SEQ1:-0}" --argjson led0 "${E08_LED0:-0}" --argjson led1 "${E08_LED1:-0}" '
    {attempts: length, rejected: (map(select(.outcome == "denied")) | length),
     # NATS cierra el socket justo después de "-ERR Authorization Violation": sin credencial el cliente a veces ve el reset antes que el error
     rejected_by_authorization: (map(select(.outcome == "denied" and (.class == "acl" or .class == "authn" or (.case == "nocred" and .class == "network")))) | length),
     per_case: (group_by(.case) | map({key: .[0].case, value: {attempts: length, rejected: (map(select(.outcome == "denied")) | length)}}) | from_entries),
     stream_seq_delta: ($seq1 - $seq0), billing_total_delta: ($led1 - $led0)}' "$OUT/raw/authz.json"
}

collect() {
  _t0=$1
  _t1=$2
  _dir=$3
  mesh_ledger "$_dir/raw/ledger.json" || return 1
  cp "$_dir/raw/ledger.json" "$_dir/ledger_final.json"
  printf 'ts,phase,region,matchId,emit_lag_s,e2e_lag_s,count\n' >"$_dir/billing_latency.csv"
  for _t in n-r1 n-r2 p-r1 p-r2; do
    e08_latency_rows "$_t" "${_t#*-}" >>"$_dir/billing_latency.csv"
  done
  _nr1=$(e08_tag_stats n-r1 "$_dir/raw/ledger.json")
  _nr2=$(e08_tag_stats n-r2 "$_dir/raw/ledger.json")
  _pr1=$(e08_tag_stats p-r1 "$_dir/raw/ledger.json")
  _pr2=$(e08_tag_stats p-r2 "$_dir/raw/ledger.json")
  _neg=$(e08_negatives_json)

  # timeline de /leafz
  jq -s '.' "$_dir/raw/timeline.jsonl" >"$_dir/leafz_timeline.json"
  jq -r '["ts", "global_leafs", "r2_leafs", "device_total"], (.[] | [.ts, .global_leafs, .r2_leafs, .device_total]) | @csv' "$_dir/leafz_timeline.json" >"$_dir/leafz_timeline.csv"
  cp "$_dir/raw/leafz-global-partition.json" "$_dir/leafz_capture_partition.json" 2>/dev/null || true
  _tl=$(jq -c --arg cut "$E08_T_CUT" --arg heal "$E08_T_HEAL" --arg drained "$E08_T_DRAINED" '
    {samples: length,
     down_samples: (map(select(.global_leafs == 1 and .r2_leafs == 0)) | length),
     down_first: (map(select(.global_leafs == 1 and .r2_leafs == 0)) | .[0].ts),
     up_after_heal: (map(select(.ts > $heal and .global_leafs == 2 and .r2_leafs == 1)) | .[0].ts),
     final_global_leafs: (.[-1].global_leafs), final_r2_leafs: (.[-1].r2_leafs)}' "$_dir/leafz_timeline.json")

  # transferencia local de r2 durante el corte
  _cut_s=$(mesh_epoch "$E08_T_CUT")
  _heal_s=$(mesh_epoch "$E08_T_HEAL")
  _during=$(jq -n --rawfile M "$_dir/raw/matches-p-r2.psv" --slurpfile D "$_dir/raw/device-r2-at-heal.json" --slurpfile F "$_dir/raw/device-r2-final.json" \
    --argjson cut "$_cut_s" --argjson heal "$_heal_s" --argjson margin "$E08_LOCAL_MARGIN_S" --argjson before "$E08_DEV_BEFORE" '
    ($M | split("\n") | map(select(length > 0) | split("|") | {id: .[0], at: (.[1] | tonumber)})) as $m
    | ($m | map(select(.at >= $cut and .at <= ($heal - $margin)))) as $due
    | ($due | map(select(($D[0].byMatchId[.id] // 0) >= 1)) | length) as $done
    | {matches_created_during_cut: ($m | map(select(.at >= $cut and .at <= $heal)) | length),
       matches_due: ($due | length), transferred_by_heal: $done,
       local_ratio: (if ($due | length) == 0 then null else $done / ($due | length) end),
       device_growth_during_cut: ($D[0].distinct - $before.distinct),
       device_final_ratio: ($m | map(select(($F[0].byMatchId[.id] // 0) >= 1)) | length) / ($m | length),
       device_duplicates_final: ($m | map(select(($F[0].byMatchId[.id] // 0) > 1)) | length)}')

  # retención del bridge: lag del grupo mesh-bridge de r2 durante el corte
  _lag=$(awk -F, -v cut="$E08_T_CUT" -v heal="$E08_T_HEAL" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "lag_r2") c = i; next }
    c && $c != "" { v = $c + 0; if ($1 >= cut && $1 <= heal && v > mx) mx = v; if ($1 > heal && v > mxa) mxa = v; last = v; n++ }
    END { printf "{\"lag_r2_max_during_cut\":%d,\"lag_r2_max_after_heal\":%d,\"lag_r2_last\":%d,\"samples\":%d}\n", mx, mxa, last, n }' "$_dir/raw/lag.csv")

  _drain_s=$(expctl elapsed "$E08_T_HEAL" "$(printf '%s' "$_pr2" | jq -r '.last_received_at')")
  _rej=$((E08_REJ1 - E08_REJ0))

  jq -n --arg run "$E08_RUN" --arg pfx "$E08_PFX" \
    --argjson nr1 "$_nr1" --argjson nr2 "$_nr2" --argjson pr1 "$_pr1" --argjson pr2 "$_pr2" \
    --argjson neg "$_neg" --argjson tl "$_tl" --argjson during "$_during" --argjson lag "$_lag" \
    --argjson drain "$_drain_s" --argjson rej "$_rej" \
    --slurpfile s1 "$_dir/raw/meter-n-r1.log.summary.json" --slurpfile s2 "$_dir/raw/meter-n-r2.log.summary.json" \
    --arg cut "$E08_T_CUT" --arg heal "$E08_T_HEAL" --arg link "$E08_T_LINK" --arg drained "$E08_T_DRAINED" \
    --arg n0 "$E08_T_N0" --arg n1 "$E08_T_N1" '
    {run: $run, prefix: $pfx,
     normal: {r1: ($nr1 + {expected_pairs: $s1[0].expectedPairs, match_ratio: ($nr1.matches / $s1[0].expectedPairs), load_published: $s1[0].published, load_late: $s1[0].late, load_dropped: $s1[0].dropped}),
              r2: ($nr2 + {expected_pairs: $s2[0].expectedPairs, match_ratio: ($nr2.matches / $s2[0].expectedPairs), load_published: $s2[0].published, load_late: $s2[0].late, load_dropped: $s2[0].dropped}),
              false_rejects: $rej, window: {start: $n0, end: $n1}},
     negatives: $neg,
     partition: ($during + $lag + {r2: $pr2, r1: $pr1, leafz: $tl,
        cut: $cut, heal: $heal, link_restored: $link, drained: $drained,
        duration_s: ((($heal | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) - ($cut | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601))),
        drain_s: $drain, billing_duplicates: ($pr2.duplicates + $pr1.duplicates + $nr1.duplicates + $nr2.duplicates)})}' >"$_dir/metrics.json" || return 1

  # evidencia: logs de nats-server y contadores del mesh
  for _n in r1 r2 global; do
    k -n "voltera-$_n" logs deploy/nats -c nats --tail=300 >"$_dir/logs/nats-server-$_n.log" 2>&1 || true
  done
  k -n voltera-global logs deploy/billing-sim -c billing-sim --tail=100 >"$_dir/logs/billing-sim.log" 2>&1 || true
  for _r in r1 r2; do
    k -n "voltera-$_r" logs deploy/mesh-bridge -c mesh-bridge --tail=300 >"$_dir/logs/mesh-bridge-$_r.log" 2>&1 || true
  done
  expctl prom range 'sum by (consumergroup, topic) (kafka_consumergroup_lag_sum{consumergroup="mesh-bridge"})' \
    --start "$E08_T_N0" --end "$E08_T_DRAINED" --step 5s --out "$_dir/prom/mesh_bridge_lag.json" >/dev/null 2>&1 || true
}
