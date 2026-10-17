#!/bin/sh
# E7: pérdida de un mensaje de posición sin expulsar al medidor (TTL). Se carga con `.` desde experiments/run.sh.
# Define exp_reset, drive y collect. Corrida rápida: P = 10 s, TTL = 25 s (2,5 ciclos), M = 200 medidores en r1.
# Durante el experimento VIEW_TTL_S del proyector vale 25 (kubectl set env); el trap lo restaura a $VIEW_TTL_S (150).
# shellcheck disable=SC2154  # OUT, EXPCTL, VIEW_TTL_S los define el runner / params.env

# shellcheck source=experiments/lib/probejob.sh
. "$HERE/lib/probejob.sh"
# shellcheck source=experiments/lib/redisview.sh
. "$HERE/lib/redisview.sh"

E7_NS=voltera-r1
E7_REGION=r1
E7_M=200
E7_P=10
E7_TTL=25
E7_LOAD_S=420          # el Job de carga se borra al terminar el experimento
E7_PF_PORT=19850       # port-forward propio al proyector (rango 19850-19899)
E7_PF_PID=
E7_JOB=
E7_CLEANED=0
E7_PAUSED=0
E7_TTL_CHANGED=0

e7_cleanup() {
  [ "$E7_CLEANED" -eq 0 ] || return 0
  E7_CLEANED=1
  rv_watch_stop
  e7_fp off >/dev/null 2>&1 || true
  [ -z "$E7_PF_PID" ] || kill "$E7_PF_PID" 2>/dev/null || true
  [ -z "$E7_JOB" ] || k -n "$E7_NS" delete job "$E7_JOB" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  [ "$E7_PAUSED" -eq 0 ] || chaos_resume_keda "$E7_NS" declaration-gateway || true
  if [ "$E7_TTL_CHANGED" -eq 1 ]; then
    log "restaura VIEW_TTL_S=$VIEW_TTL_S en market-projector"
    k -n "$E7_NS" set env deploy/market-projector "VIEW_TTL_S=$VIEW_TTL_S" >/dev/null || true
    k -n "$E7_NS" rollout status deploy/market-projector --timeout=120s >/dev/null || true
  fi
}

# e7_fp <modo>: fija el failpoint drop_position por el port-forward propio (latencia de ms); imprime el código HTTP
e7_fp() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$E7_PF_PORT/admin/failpoint?name=drop_position&mode=$1"
}

e7_fp_must() {
  _code=$(e7_fp "$1")
  [ "$_code" = 200 ] || { log "failpoint drop_position=$1 devolvió HTTP $_code"; return 1; }
}

# e7_wait_full <timeout_s> <muestras> <desde_us>: espera a que las últimas <muestras> muestras, todas posteriores a
# <desde_us> (reloj de Redis), tengan keys == M. Sin el filtro por instante, las muestras anteriores a la inyección
# (todavía con M keys) darían por buena la recuperación antes de empezar.
e7_wait_full() {
  _need=${2:-3}
  _t=0
  while [ "$_t" -lt "$1" ]; do
    if tail -n "$_need" "$WATCH" | awk -F';' -v m="$E7_M" -v need="$_need" -v after="${3:-0}" \
      '{ n = split($2, a, " "); if (n >= 3 && a[2] == m && a[1] >= after) ok++ } END { exit !(ok == need) }'; then
      return 0
    fi
    sleep 1
    _t=$((_t + 1))
  done
  return 1
}

# e7_mark <nombre>: añade el instante del reloj de Redis (µs) a raw/markers.csv y lo deja en MARK_US
e7_mark() {
  MARK_US=$(rv_now_us) || return 1
  printf '%s,%s\n' "$1" "$MARK_US" >>"$OUT/data/markers.csv"
}

exp_reset() {
  trap 'e7_cleanup; cluster_unlock; exit 130' INT TERM
  trap 'e7_cleanup; cluster_unlock' EXIT
  need curl
  need awk
  probejob_build || return 1
  # TTL de la corrida rápida y gateway fijo en 1 réplica (20 decl/s < C_POD): sin arranques en frío de KEDA a mitad
  k -n "$E7_NS" set env deploy/market-projector "VIEW_TTL_S=$E7_TTL" >/dev/null || return 1
  E7_TTL_CHANGED=1
  k -n "$E7_NS" rollout status deploy/market-projector --timeout=120s >/dev/null || return 1
  chaos_pause_keda "$E7_NS" declaration-gateway 1 || return 1
  E7_PAUSED=1
  wait_ready "$E7_NS" declaration-gateway 120 || return 1
  pod=$(k -n "$E7_NS" get pod -l app=market-projector --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}') || return 1
  kubectl --context "$KUBE_CONTEXT" -n "$E7_NS" port-forward "pod/$pod" "$E7_PF_PORT:9090" >/dev/null 2>&1 &
  E7_PF_PID=$!
  _i=0
  until curl -sf "http://127.0.0.1:$E7_PF_PORT/healthz" >/dev/null 2>&1; do
    _i=$((_i + 1))
    [ "$_i" -lt 30 ] || { log "sin acceso al proyector por port-forward"; return 1; }
    sleep 1
  done
  e7_fp_must off || return 1
}

drive() {
  RUN=$(date -u +%y%m%d%H%M%S)
  PFX=e07-$RUN-
  GLOB="pos:*:$PFX*"
  WATCH=$OUT/raw/redis_watch.txt
  mkdir -p "$OUT/data"
  printf 'name,us\n' >"$OUT/data/markers.csv"
  printf '%s\n' "$PFX" >"$OUT/raw/prefix.txt"

  rv_watch_start "$GLOB" "$WATCH" 1
  probejob_start "$E7_REGION" amqp --profile steady --prefix "$PFX" --meters "$E7_M" --period "$E7_P" \
    --duration "$E7_LOAD_S" --seed 7 || return 1
  E7_JOB=$PROBEJOB_LAST
  log "carga steady $E7_JOB: $E7_M medidores, ciclo ${E7_P}s, prefijo $PFX"

  e7_wait_full 120 5 || { log "las $E7_M keys no se estabilizaron en 120 s"; return 1; }
  log "calentamiento listo: $E7_M keys"
  sleep $((E7_P * 2))

  # 7a: se pierde un ciclo (9,5 s < P: ningún medidor pierde dos mensajes seguidos). sleep fraccional: GNU y BSD lo aceptan
  log "7a: drop_position=drop durante 9,5 s"
  e7_fp_must drop || return 1
  e7_mark a_on || return 1
  sleep 9.5
  e7_fp_must off || return 1
  e7_mark a_off || return 1
  e7_wait_full 60 5 "$MARK_US" || { log "7a: las keys no volvieron a $E7_M"; return 1; }
  e7_mark a_end || return 1
  sleep $((E7_P * 2))

  # 7b: se pierden 3 ciclos (30 s > TTL): las keys vencen a TTL del último refresco
  log "7b: drop_position=drop durante 30 s"
  e7_fp_must drop || return 1
  e7_mark b_on || return 1
  sleep 30
  e7_fp_must off || return 1
  e7_mark b_off || return 1
  e7_wait_full 60 5 "$MARK_US" || { log "7b: las keys no volvieron a $E7_M"; return 1; }
  e7_mark b_end || return 1
  sleep $((E7_P * 2))

  # 7c: se pierden las keys del prefijo (no FLUSHALL)
  log "7c: borrado de las keys $GLOB"
  DELETED=$(rv_delete "$GLOB") || return 1
  e7_mark c_del || return 1
  e7_wait_full 60 5 "$MARK_US" || { log "7c: las keys no volvieron a $E7_M"; return 1; }
  e7_mark c_end || return 1
  sleep 5
  rv_watch_stop
}

# e7_marker <nombre>: µs del marcador
e7_marker() { awk -F, -v n="$1" '$1 == n { print $2 }' "$OUT/data/markers.csv"; }

# e7_window_stats <desde_us> <hasta_us>: "min_keys min_pttl_ms" en la ventana (min_pttl solo con keys presentes)
e7_window_stats() {
  awk -F';' -v from="$1" -v to="$2" '{
    n = split($2, a, " "); if (n < 3 || a[1] < from || a[1] > to) next
    if (!s1 || a[2] < mk) mk = a[2]
    s1 = 1
    if (a[2] > 0 && (!s2 || a[3] < mt)) { mt = a[3]; s2 = 1 }
  } END { print (s1 ? mk : -1), (s2 ? mt : -1) }' "$WATCH"
}

# e7_first_full <desde_us>: segundos hasta la primera muestra con keys == M (-1 si no hay)
e7_first_full() {
  awk -F';' -v after="$1" -v m="$E7_M" '{
    n = split($2, a, " "); if (n < 3) next
    if (a[1] >= after && a[2] == m) { printf "%.3f\n", (a[1] - after) / 1e6; found = 1; exit }
  } END { if (!found) print -1 }' "$WATCH"
}

# e7_expiry <on_us> <off_us> <ttl_s>: por key, error = desaparición observada - (projectedAt + TTL), en segundos
e7_expiry() {
  awk -F';' -v from="$1" -v to="$2" -v ttl="$3" '
    BEGIN { print "key,last_seen_us,projected_us,vanish_us,expected_us,error_s" }
    {
      n = split($2, a, " "); if (n < 3 || a[1] < from || a[1] > to) next
      t = a[1]
      delete cur
      for (i = 4; i <= n; i++) { split(a[i], f, ","); cur[f[1]] = 1; lastseen[f[1]] = t; proj[f[1]] = f[3]; seen[f[1]] = 1 }
      for (k in seen) if (!(k in cur) && !(k in gone)) {
        gone[k] = t
        expiry = proj[k] + ttl * 1000000
        printf "%s,%.0f,%.0f,%.0f,%.0f,%.3f\n", k, lastseen[k], proj[k], t, expiry, (t - expiry) / 1000000
      }
    }' "$WATCH"
}

# e7_drop_stats <desde_us> <hasta_us>: "meterId_distintos max_por_medidor total" de las líneas dropped del proyector
e7_drop_stats() {
  awk -F, -v from="$1" -v to="$2" 'NR > 1 && $2 >= from && $2 <= to { c[$1]++; tot++ }
    END { m = 0; d = 0; for (k in c) { d++; if (c[k] > m) m = c[k] } print d + 0, m, tot + 0 }' "$OUT/data/drops.csv"
}

collect() {
  WATCH=$OUT/raw/redis_watch.txt

  # líneas dropped del proyector (meterId, µs con resolución de segundo)
  k -n "$E7_NS" logs deploy/market-projector -c market-projector 2>/dev/null >"$OUT/logs/market-projector.log" || true
  {
    printf 'meterId,us\n'
    jq -r 'select(.result == "dropped") | [.meterId, ((.ts | sub("\\.[0-9]+Z$"; "Z") | fromdate) * 1000000)] | @csv' \
      "$OUT/logs/market-projector.log" 2>/dev/null | tr -d '"'
  } >"$OUT/data/drops.csv"

  a_on=$(e7_marker a_on) a_off=$(e7_marker a_off) a_end=$(e7_marker a_end)
  b_on=$(e7_marker b_on) b_off=$(e7_marker b_off)
  c_del=$(e7_marker c_del)

  # 7a: ventana desde 5 s antes del drop hasta que las keys vuelven a M; los logs tienen resolución de 1 s: ventana +-1 s
  read -r a_min_keys a_min_ttl_ms <<EOS
$(e7_window_stats $((a_on - 5000000)) "$a_end")
EOS
  read -r a_dropped_meters a_max_drops a_dropped <<EOS
$(e7_drop_stats $((a_on - 1000000)) $((a_off + 1000000)))
EOS
  a_recover=$(e7_first_full "$a_off")

  # 7b
  e7_expiry "$b_on" "$b_off" "$E7_TTL" >"$OUT/data/expiry_7b.csv"
  read -r b_min_keys _ <<EOS
$(e7_window_stats "$b_on" "$b_off")
EOS
  b_reappear=$(e7_first_full "$b_off")
  read -r _ _ b_dropped <<EOS
$(e7_drop_stats $((b_on - 1000000)) $((b_off + 1000000)))
EOS

  # 7c
  c_repop=$(e7_first_full "$c_del")

  # figura: keys y ventanas de pérdida (drop_window = M mientras el failpoint está activo)
  awk -F';' -v m="$E7_M" -v aon="$a_on" -v aoff="$a_off" -v bon="$b_on" -v boff="$b_off" 'BEGIN { print "ts,keys,min_ttl_s,drop_window" }
    NF >= 2 { n = split($2, a, " "); if (n < 3) next
      w = ((a[1] >= aon && a[1] <= aoff) || (a[1] >= bon && a[1] <= boff)) ? m : 0
      printf "%s,%d,%s,%d\n", $1, a[2], (a[3] < 0 ? "" : a[3] / 1000), w }' "$WATCH" >"$OUT/raw/redis_keys.csv"

  ttl_cfg=$(expctl http-get "$E7_NS/market-projector:9090/debug/state" | jq -r '.config.viewTtlSec // 0')
  jq -n \
    --argjson m "$E7_M" --argjson p "$E7_P" --argjson ttl "${ttl_cfg:-0}" --arg prefix "$PFX" \
    --argjson a_min_keys "$a_min_keys" --argjson a_min_ttl_ms "$a_min_ttl_ms" --argjson a_dropped "$a_dropped" \
    --argjson a_dropped_meters "$a_dropped_meters" --argjson a_max_drops "$a_max_drops" \
    --argjson a_window "$((a_off - a_on))" --argjson a_recover "$a_recover" \
    --argjson b_min_keys "$b_min_keys" --argjson b_reappear "$b_reappear" --argjson b_dropped "$b_dropped" \
    --argjson b_window "$((b_off - b_on))" \
    --argjson c_deleted "${DELETED:-0}" --argjson c_repop "$c_repop" \
    --slurpfile expiry "$(e7_expiry_json)" \
    '{setup: {meters: $m, period_s: $p, view_ttl_s: $ttl, prefix: $prefix},
      a: {min_keys: $a_min_keys, min_ttl_s: ($a_min_ttl_ms / 1000), dropped: $a_dropped, dropped_meters: $a_dropped_meters,
          max_drops_per_meter: $a_max_drops, drop_window_s: ($a_window / 1000000), recover_s: $a_recover},
      b: ({min_keys: $b_min_keys, reappear_s: $b_reappear, dropped: $b_dropped, drop_window_s: ($b_window / 1000000)} + $expiry[0]),
      c: {deleted: $c_deleted, repopulate_s: $c_repop}}' >"$OUT/metrics.json"
}

# e7_expiry_json: resume expiry_7b.csv en {vanished, err_min_s, err_max_s} y devuelve la ruta del JSON
e7_expiry_json() {
  _f=$OUT/data/expiry_7b.json
  awk -F, 'NR > 1 { n++; if (n == 1 || $6 < mn) mn = $6; if (n == 1 || $6 > mx) mx = $6 }
    END { if (n == 0) { mn = 0; mx = 0 } printf "{\"vanished\": %d, \"err_min_s\": %s, \"err_max_s\": %s}\n", n, mn, mx }' \
    "$OUT/data/expiry_7b.csv" >"$_f"
  printf '%s\n' "$_f"
}
