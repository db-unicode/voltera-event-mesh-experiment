#!/bin/sh
# E10: seguimiento de un evento de punta a punta con KEDA en el camino. Se carga con `.` desde experiments/run.sh.
# declaration-gateway y energy-transfer de r1 parten de 0 réplicas; meter-sim envía un par SURPLUS+DEFICIT en una zona
# única y `expctl trace` reconstruye la cadena por los eventId de las declaraciones y por el matchId.
# shellcheck shell=sh disable=SC2329,SC2034

# shellcheck source=experiments/lib/metersim.sh
. "$HERE/lib/metersim.sh"
# shellcheck source=experiments/lib/mesh.sh
. "$HERE/lib/mesh.sh"

E10_DIR=$HERE/$EXP_ID
E10_NS=voltera-r1
E10_RUN=$(date -u +%d%H%M%S)
E10_PFX=e10-$E10_RUN-
E10_ZERO_LIMIT_S=${E10_ZERO_LIMIT_S:-240}
E10_DONE_LIMIT_S=${E10_DONE_LIMIT_S:-240}
E10_STOP=$OUT/raw/.stop-pin
E10_BG=

e10_cleanup() {
  : >"$E10_STOP" 2>/dev/null || true
  for _p in $E10_BG; do kill "$_p" 2>/dev/null || true; done
  chaos_resume_keda "$E10_NS" declaration-gateway >/dev/null 2>&1 || true
  chaos_resume_keda "$E10_NS" energy-transfer >/dev/null 2>&1 || true
  [ "${E10_KEEP_TOOLS:-0}" = 1 ] || mesh_tools_down
}
trap 'e10_cleanup; cluster_unlock' EXIT
trap 'exit 130' INT TERM

e10_replicas() { k -n "$E10_NS" get deploy "$1" -o jsonpath='{.status.replicas}' 2>/dev/null; }

e10_both_zero() {
  _g=$(e10_replicas declaration-gateway)
  _t=$(e10_replicas energy-transfer)
  [ "${_g:-0}" = 0 ] && [ "${_t:-0}" = 0 ]
}

e10_no_pods() { [ "$(k -n "$E10_NS" get pods -l 'app in (declaration-gateway,energy-transfer)' -o name | wc -l | tr -d ' ')" = 0 ]; }

e10_zone_empty() { [ "$(k -n "$E10_NS" exec deploy/market-view -c redis -- redis-cli --scan --pattern 'pos:z-1:*' | wc -l | tr -d ' ')" = 0 ]; }

exp_reset() {
  if [ "${SKIP_BUILD:-0}" != 1 ]; then
    MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/images/build" meter-sim >"$OUT/logs/images-build.log" 2>&1 \
      || { cat "$OUT/logs/images-build.log" >&2; return 1; }
  fi
  mesh_tools_up || return 1
  chaos_resume_keda "$E10_NS" declaration-gateway >/dev/null 2>&1 || true
  chaos_resume_keda "$E10_NS" energy-transfer >/dev/null 2>&1 || true
  for _d in market-projector matching-engine mesh-bridge device-sim; do
    mesh_wait_deploy "$E10_NS" "$_d" 120 || return 1
  done
  mesh_wait_deploy voltera-global billing-sim 120 || return 1
  log "esperando gateway y energy-transfer en 0 réplicas (cooldown de KEDA)"
  mesh_retry "$E10_ZERO_LIMIT_S" e10_both_zero || { log "el gateway o energy-transfer no llegan a 0 (¿hay tráfico en r1?)"; return 1; }
  # los pods terminando también cuentan: se espera a que no quede ninguno
  mesh_retry 60 e10_no_pods || { log "quedan pods del gateway o de energy-transfer"; return 1; }
  # zona única: la vista de r1 no debe tener posiciones ajenas en z-1 (TTL 150 s)
  mesh_retry 170 e10_zone_empty || log "aviso: pos:z-1:* no está vacío; el par podría emparejarse con una posición ajena (se verifica al final)"
}

# e10_pin: apenas el gateway y energy-transfer tienen un pod, fija KEDA en 1 réplica para que sus logs sigan ahí
# cuando termine la cadena (a 0 réplicas los logs desaparecen). No afecta al arranque en frío, que ya ocurrió.
e10_pin() {
  _gw=0
  _et=0
  while [ ! -e "$E10_STOP" ]; do
    if [ "$_gw" = 0 ] && [ "$(e10_replicas declaration-gateway)" -ge 1 ] 2>/dev/null; then
      chaos_pause_keda "$E10_NS" declaration-gateway 1 >/dev/null 2>&1 && _gw=1
    fi
    if [ "$_et" = 0 ] && [ "$(e10_replicas energy-transfer)" -ge 1 ] 2>/dev/null; then
      chaos_pause_keda "$E10_NS" energy-transfer 1 >/dev/null 2>&1 && _et=1
    fi
    sleep 1
  done
}

drive() {
  rm -f "$E10_STOP"
  e10_pin &
  E10_BG="$E10_BG $!"
  E10_T_SEND=$(expctl now)
  log "E10: un par SURPLUS+DEFICIT (prefijo $E10_PFX) con gateway y energy-transfer en 0"
  metersim_run r1 --profile pairs --zones 1 --vendor voltix-2 --meters 2 --rate 2 --duration 1s --prefix "$E10_PFX" \
    --log-publish >"$OUT/raw/meter-sim.log" 2>&1 || return 1
  metersim_summary <"$OUT/raw/meter-sim.log" >"$OUT/raw/meter-sim.summary.json"
  jq -e '.published == 2 and .errors == 0' "$OUT/raw/meter-sim.summary.json" >/dev/null || return 1
  # eventId de las dos declaraciones, de los logs de etapa de meter-sim
  jq -R -c 'fromjson? | select(.stage == "publish" and .result == "ok" and .eventId != null)' "$OUT/raw/meter-sim.log" >"$OUT/raw/declarations.jsonl"
  E10_D1=$(jq -r 'select(.side == "SURPLUS") | .eventId' "$OUT/raw/declarations.jsonl" | head -n 1)
  E10_D2=$(jq -r 'select(.side == "DEFICIT") | .eventId' "$OUT/raw/declarations.jsonl" | head -n 1)
  [ -n "$E10_D1" ] && [ -n "$E10_D2" ] || { log "no se hallaron los eventId de las declaraciones en el log de meter-sim"; return 1; }
  echo "$E10_D1 $E10_D2" >"$OUT/raw/declaration-ids.txt"

  _el=0
  E10_MATCH=
  while [ "$_el" -lt "$E10_DONE_LIMIT_S" ]; do
    if [ -z "$E10_MATCH" ]; then
      mesh_pg r1 matches-db "select match_id || '|' || seller_meter_id || '|' || buyer_meter_id from matches where seller_meter_id like '$E10_PFX%' or buyer_meter_id like '$E10_PFX%'" >"$OUT/raw/match.psv" 2>/dev/null || true
      E10_MATCH=$(head -n 1 "$OUT/raw/match.psv" | cut -d'|' -f1)
    fi
    if [ -n "$E10_MATCH" ]; then
      _b=$(tb curl -fsS --max-time 10 "http://billing-sim.voltera-global:8080/state?detail=1" | jq --arg m "$E10_MATCH" '.byMatchId[$m].count // 0' 2>/dev/null || echo 0)
      _d=$(tb curl -fsS --max-time 10 "http://device-sim.voltera-r1:8080/state" | jq --arg m "$E10_MATCH" '.byMatchId[$m] // 0' 2>/dev/null || echo 0)
      [ "${_b:-0}" -ge 1 ] && [ "${_d:-0}" -ge 1 ] && break
    fi
    sleep 2
    _el=$((_el + 2))
  done
  E10_T_DONE=$(expctl now)
  [ -n "$E10_MATCH" ] && [ "${_b:-0}" -ge 1 ] && [ "${_d:-0}" -ge 1 ] || { log "la cadena no terminó en ${E10_DONE_LIMIT_S}s (match=$E10_MATCH billing=${_b:-0} device=${_d:-0})"; return 1; }
  sleep 3   # los últimos logs llegan al log del pod
}

e10_trace() { # <archivo base> <ids...>: tabla y JSON
  _tb=$1
  shift
  expctl trace "$@" --namespaces voltera-r1,voltera-global --since 20m --out "$_tb.json" >"$_tb.txt" 2>"$_tb.err" || return 1
}

collect() {
  _dir=$3
  # ronda 1: por los eventId de las declaraciones y el matchId; de ahí sale el eventId del match (log de commit del motor)
  e10_trace "$_dir/raw/trace-round1" "$E10_D1" "$E10_D2" "$E10_MATCH" || return 1
  E10_EVENT=$(jq -r --arg m "$E10_MATCH" '[.hops[] | select(.service == "matching-engine" and .stage == "commit" and .line.matchId == $m)][0].eventId // empty' "$_dir/raw/trace-round1.json")
  [ -n "$E10_EVENT" ] || { log "no hay log de commit del motor para $E10_MATCH"; return 1; }
  # ronda 2: las cuatro claves juntas; evidencia por cada clave pedida (declaración, matchId, eventId del match)
  e10_trace "$_dir/trace_all" "$E10_D1" "$E10_D2" "$E10_MATCH" "$E10_EVENT" || return 1
  e10_trace "$_dir/trace_$E10_D1" "$E10_D1" || true
  e10_trace "$_dir/trace_$E10_D2" "$E10_D2" || true
  e10_trace "$_dir/trace_$E10_MATCH" "$E10_MATCH" || true
  jq -f "$E10_DIR/analyze.jq" --arg d1 "$E10_D1" --arg d2 "$E10_D2" --arg match "$E10_MATCH" --arg ev "$E10_EVENT" \
    "$_dir/trace_all.json" >"$_dir/raw/chain.json" || return 1

  jq -R -s '(split("\n") | map(select(length > 0) | split("|"))) as $r
    | {rows: ($r | length), matchId: $r[0][0], seller: $r[0][1], buyer: $r[0][2]}' "$_dir/raw/match.psv" >"$_dir/raw/match.json" || return 1

  # eventos de k8s del arranque en frío (gateway y energy-transfer) y de KEDA
  k -n "$E10_NS" get events -o json | jq --arg t "$E10_T_SEND" '
    def epoch: (sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601);
    [.items[] | select(.involvedObject.name | test("declaration-gateway|energy-transfer"))
     | {t: (.eventTime // .lastTimestamp // .firstTimestamp), kind: .involvedObject.kind, name: .involvedObject.name, reason, message, count}
     | select((.t | epoch) >= ($t | epoch) - 2)] | sort_by(.t)' >"$_dir/raw/k8s-events.json" || return 1
  jq -r '.[] | [.t, .kind, .name, .reason, .message] | @tsv' "$_dir/raw/k8s-events.json" >"$_dir/keda_events.txt"
  k -n "$E10_NS" get scaledobject declaration-gateway energy-transfer -o wide >>"$_dir/keda_events.txt" 2>&1 || true

  # anclas del arranque de cada pod: creación (k8s, 1 s de resolución), arranque de la app (log, ms) y primer mensaje
  for _w in gateway:declaration-gateway:gateway_consume:sent transfer:energy-transfer:transfer_consume:match_commit; do
    _tag=${_w%%:*}
    _rest=${_w#*:}
    _app=${_rest%%:*}
    _rest=${_rest#*:}
    _hop=${_rest%%:*}
    _from=${_rest#*:}
    _pod=$(jq -r --arg h "$_hop" '.chain[] | select(.name == $h) | .pod' "$_dir/raw/chain.json")
    [ -n "$_pod" ] || continue
    k -n "$E10_NS" get pod "$_pod" -o json | jq '{pod: .metadata.name, created: .metadata.creationTimestamp,
      scheduled: ([.status.conditions[]? | select(.type == "PodScheduled") | .lastTransitionTime][0]),
      ready: ([.status.conditions[]? | select(.type == "Ready") | .lastTransitionTime][0])}' >"$_dir/raw/pod-$_tag.json"
    k -n "$E10_NS" logs "$_pod" -c "$_app" --timestamps 2>/dev/null | head -n 30 \
      | jq -R -c --arg app "$_app" 'split(" ") as $p | ($p[1:] | join(" ") | fromjson?) as $l | select($l != null) | {kts: $p[0], msg: $l.msg, stage: $l.stage, ts: ($l.ts // $p[0])}' \
      | jq -s '[.[] | select(.stage == "start" or (.msg // "" | test("start")))][0]' >"$_dir/raw/appstart-$_tag.json"
  done

  # descomposición del arranque en frío y métricas
  jq -n --slurpfile C "$_dir/raw/chain.json" --slurpfile G "$_dir/raw/pod-gateway.json" --slurpfile GA "$_dir/raw/appstart-gateway.json" \
    --slurpfile TP "$_dir/raw/pod-transfer.json" --slurpfile TA "$_dir/raw/appstart-transfer.json" --slurpfile EV "$_dir/raw/k8s-events.json" \
    --slurpfile M "$_dir/raw/match.json" --arg pfx "$E10_PFX" --arg send "$E10_T_SEND" --arg doneat "$E10_T_DONE" \
    --arg d1 "$E10_D1" --arg d2 "$E10_D2" --arg match "$E10_MATCH" --arg ev "$E10_EVENT" -f "$E10_DIR/metrics.jq" >"$_dir/metrics.json" 2>"$_dir/raw/metrics.err" \
    || { cat "$_dir/raw/metrics.err" >&2; return 1; }

  jq -r '["ts", "offset_s", "service", "stage", "name", "broker"], (.chain | .[] | [.ts, .offset_s, .service, .stage, .name, .broker]) | @csv' "$_dir/raw/chain.json" >"$_dir/hops.csv"
}
