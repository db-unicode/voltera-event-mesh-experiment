#!/bin/sh
# E9: Zero Trust sobre el bus. Se carga con `.` desde experiments/run.sh (define drive y collect).
# Una celda = identidad x canal x operación (zt-matrix.csv). Cada identidad es un Job con el ServiceAccount real del servicio
# (más una SA ajena, ladrones de credencial y un pod sin sidecar) que intenta sus celdas con zt-probe.
# shellcheck shell=sh disable=SC2329,SC2034

# shellcheck source=experiments/lib/metersim.sh
. "$HERE/lib/metersim.sh"
# shellcheck source=experiments/lib/mesh.sh
. "$HERE/lib/mesh.sh"

E09_DIR=$HERE/$EXP_ID
E09_RUN=$(date -u +%d%H%M%S)
E09_PLAIN_NS=e09-plain
E09_PFX=e9-$E09_RUN
E09_SETTLE_S=${E09_SETTLE_S:-20}   # propagación de las políticas a los sidecars

# e09_identities: nombres de la primera columna de zt-identities.csv
e09_identities() { awk -F, 'NR > 1 { print $1 }' "$E09_DIR/zt-identities.csv"; }

# e09_field <identidad> <n>: campo n (1-based) de la fila de la identidad
e09_field() { awk -F, -v id="$1" -v n="$2" '$1 == id { print $n }' "$E09_DIR/zt-identities.csv"; }

e09_cells() { # identidad -> id,broker,op,target
  awk -F, -v id="$1" 'NR > 1 && $2 == id { print $1 "," $3 "," $5 "," $6 }' "$E09_DIR/zt-matrix.csv"
}

# e09_job_env <identidad>: variables del Job (AMQP_*, NATS_*) según las credenciales de la fila
e09_job_env() {
  _je_amqp=$(e09_field "$1" 5)
  _je_nats=$(e09_field "$1" 6)
  _je_node=$(e09_field "$1" 7)
  printf 'NATS_URL=nats://nats.voltera-%s:4222\n' "$_je_node"
  case $_je_amqp in
    secret:*) printf 'AMQP_USER=secret:%s:AMQP_USER\nAMQP_PASSWORD=secret:%s:AMQP_PASSWORD\n' "${_je_amqp#secret:}" "${_je_amqp#secret:}" ;;
    literal:*)
      _je_rest=${_je_amqp#literal:}
      printf 'AMQP_USER=%s\nAMQP_PASSWORD=%s\n' "${_je_rest%%:*}" "${_je_rest#*:}"
      ;;
  esac
  case $_je_nats in
    secret:*) printf 'NATS_USER=secret:%s:NATS_USER\nNATS_PASSWORD=secret:%s:NATS_PASSWORD\n' "${_je_nats#secret:}" "${_je_nats#secret:}" ;;
    literal:*)
      _je_rest=${_je_nats#literal:}
      printf 'NATS_USER=%s\nNATS_PASSWORD=%s\n' "${_je_rest%%:*}" "${_je_rest#*:}"
      ;;
  esac
}

# e09_start <identidad> <sufijo>: crea el Job zt-<identidad><sufijo> (sin lanzar el resto)
e09_start() {
  _s_id=$1
  _s_name=zt-$_s_id$2
  e09_cells "$_s_id" >"$OUT/raw/cells/cells-$_s_name.csv"
  # shellcheck disable=SC2046  # una variable por línea, sin espacios
  set -- $(e09_job_env "$_s_id")
  mesh_probe_job "$(e09_field "$_s_id" 2)" "$_s_name" "$(e09_field "$_s_id" 3)" "$OUT/raw/cells/cells-$_s_name.csv" "$@"
}

# e09_run_all <sufijo> <identidades...>: lanza los Jobs en paralelo y espera a todos
e09_run_all() {
  _r_suffix=$1
  shift
  for _r_id in "$@"; do e09_start "$_r_id" "$_r_suffix" || return 1; done
  for _r_id in "$@"; do
    mesh_probe_wait "$(e09_field "$_r_id" 2)" "zt-$_r_id$_r_suffix" "$OUT/raw/zt$_r_suffix-$_r_id.jsonl" 400 || return 1
  done
}

e09_cleanup() {
  for _id in $(e09_identities); do
    mesh_probe_cleanup "$(e09_field "$_id" 2)" "zt-$_id"
    mesh_probe_cleanup "$(e09_field "$_id" 2)" "zt-$_id-ctl"
  done
  if [ "${E09_KEEP_POLICIES:-0}" != 1 ]; then
    k delete authorizationpolicy -A -l zero-trust=bus --ignore-not-found >/dev/null 2>&1 || true
  fi
  k delete namespace "$E09_PLAIN_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  k -n voltera-r1 delete sa zt-stranger --ignore-not-found >/dev/null 2>&1 || true
  [ "${E09_KEEP_TOOLS:-0}" = 1 ] || mesh_tools_down
}
trap 'e09_cleanup; cluster_unlock' EXIT
trap 'exit 130' INT TERM

exp_reset() {
  if [ "${SKIP_BUILD:-0}" != 1 ]; then
    MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/images/build" zt-probe meter-sim >"$OUT/logs/images-build.log" 2>&1 \
      || { cat "$OUT/logs/images-build.log" >&2; return 1; }
  fi
  mesh_tools_up || return 1
  E09_POLICIES_BEFORE=$(k get authorizationpolicy -A -l zero-trust=bus -o name | wc -l | tr -d ' ')
  k delete authorizationpolicy -A -l zero-trust=bus --ignore-not-found >/dev/null || return 1
  mesh_sa_up voltera-r1 zt-stranger
  # namespace sin sidecar con las dos credenciales válidas (meter-sim de RabbitMQ y bridge-r1 de NATS)
  k create namespace "$E09_PLAIN_NS" --dry-run=client -o yaml | k apply -f - >/dev/null || return 1
  k label namespace "$E09_PLAIN_NS" istio-injection=disabled --overwrite >/dev/null || return 1
  for _s in meter-sim-rabbitmq mesh-bridge-credentials; do
    k -n voltera-r1 get secret "$_s" -o json \
      | jq '{apiVersion, kind, metadata: {name: .metadata.name}, type, data}' \
      | k -n "$E09_PLAIN_NS" apply -f - >/dev/null || return 1
  done
  for _d in voltera-r1/mesh-bridge voltera-r2/mesh-bridge voltera-r1/matching-engine voltera-r2/matching-engine \
    voltera-r1/market-projector voltera-r2/market-projector voltera-r1/device-sim voltera-r2/device-sim voltera-global/billing-sim; do
    mesh_wait_deploy "${_d%%/*}" "${_d#*/}" 120 || return 1
  done
  k -n voltera-infra wait --for=condition=Ready pod/kafka-0 pod/rabbitmq-0 --timeout=120s >/dev/null || return 1
  mkdir -p "$OUT/raw/cells"
}

# e09_rbac <etiqueta>: líneas rbac...denied de los sidecars de los brokers, para antes/después
e09_rbac() {
  : >"$OUT/raw/rbac-$1.txt"
  for _t in voltera-infra/kafka-0 voltera-infra/rabbitmq-0 voltera-r1/nats voltera-r2/nats voltera-global/nats; do
    _ns=${_t%%/*}
    _p=${_t#*/}
    case $_p in nats) _p=$(k -n "$_ns" get pod -l app=nats -o name | head -n 1 | sed 's|^pod/||') ;; esac
    k -n "$_ns" exec "$_p" -c istio-proxy -- pilot-agent request GET stats 2>/dev/null \
      | awk -v t="$_ns/${_t#*/}" '/rbac/ && /denied/ { print t " " $0 }' >>"$OUT/raw/rbac-$1.txt" || true
  done
}

# e09_rbac_sum <etiqueta> <ns/pod-clave>: suma de los contadores denied de ese broker
e09_rbac_sum() { awk -v t="$2" 'index($1, t) == 1 { s += $NF } END { print s + 0 }' "$OUT/raw/rbac-$1.txt"; }

e09_device_missing() { # <región> <archivo de matches>
  tb curl -fsS --max-time 30 "http://device-sim.voltera-$1:8080/state" \
    | jq -r --rawfile m "$2" '. as $d | ($m | split("\n") | map(select(length > 0) | split("|")[0])) | map(select(($d.byMatchId[.] // 0) < 1)) | length'
}

drive() {
  # control sin políticas: con una credencial robada, el broker solo mira el usuario
  log "E9: control sin políticas (ladrones de credencial)"
  e09_run_all -ctl thief-rabbit thief-nats || return 1
  e09_rbac before
  E09_T_ON=$(expctl now)
  log "E9: aplica las políticas del bus"
  MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/security/bus" on >"$OUT/logs/security-on.log" 2>&1 || { cat "$OUT/logs/security-on.log" >&2; return 1; }
  k get authorizationpolicy -A -l zero-trust=bus >"$OUT/raw/policies.txt" 2>&1 || true
  sleep "$E09_SETTLE_S"

  log "E9: matriz completa ($(grep -c . "$E09_DIR/zt-matrix.csv") filas)"
  # shellcheck disable=SC2046
  e09_run_all "" $(e09_identities) || return 1
  E09_T_MATRIX=$(expctl now)

  log "E9: flujo legítimo con las políticas aplicadas"
  mesh_pairs r1 "$E09_PFX-f-r1-" 10 20 "$OUT/raw/meter-f-r1.log" &
  _p1=$!
  mesh_pairs r2 "$E09_PFX-f-r2-" 4 20 "$OUT/raw/meter-f-r2.log" &
  _p2=$!
  wait "$_p1" || return 1
  wait "$_p2" || return 1
  mesh_settle 150 "f-r1=r1=$E09_PFX-f-r1-" "f-r2=r2=$E09_PFX-f-r2-" || return 1
  E09_DEV_MISSING=-1
  _el=0
  while [ "$_el" -le 90 ]; do
    _m1=$(e09_device_missing r1 "$OUT/raw/matches-f-r1.psv" 2>/dev/null || echo 999)
    _m2=$(e09_device_missing r2 "$OUT/raw/matches-f-r2.psv" 2>/dev/null || echo 999)
    E09_DEV_MISSING=$((_m1 + _m2))
    [ "$E09_DEV_MISSING" -gt 0 ] || break
    sleep 3
    _el=$((_el + 3))
  done
  e09_rbac after
  if [ "${E09_KEEP_POLICIES:-0}" != 1 ]; then
    MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/security/bus" off >"$OUT/logs/security-off.log" 2>&1 || true
  fi
  E09_POLICIES_LEFT=$(k get authorizationpolicy -A -l zero-trust=bus -o name | wc -l | tr -d ' ')
}

collect() {
  _dir=$3
  # el control sin políticas (zt-ctl-*) repite ids de celda: no entra en la unión con la matriz
  : >"$_dir/raw/zt-all.txt"
  for _f in "$_dir"/raw/zt-*.jsonl; do
    case $_f in */zt-ctl-*) ;; *) cat "$_f" >>"$_dir/raw/zt-all.txt" ;; esac
  done
  jq -s '.' "$_dir/raw/zt-all.txt" >"$_dir/raw/zt-results.json" || return 1
  cat "$_dir"/raw/zt-ctl-*.jsonl | jq -s '.' >"$_dir/raw/zt-control.json" || return 1

  jq -R -s --slurpfile R "$_dir/raw/zt-results.json" '
    ($R[0] | map({key: .id, value: .}) | from_entries) as $res
    | (split("\n") | .[1:] | map(select(length > 0) | split(",")))
    | map({cell: .[0], identity: .[1], broker: .[2], channel: .[3], operation: .[4], target: .[5], design: .[6], expected: .[7], layer: .[8], note: .[9]}
        | . + {actual: (if $res[.cell] == null then "missing" elif $res[.cell].outcome == "allowed" then "allow" else "deny" end),
               class: ($res[.cell].class // ""), error: ($res[.cell].error // "")}
        | . + {verdict: (if .actual == "missing" then "missing" elif .actual == .expected then "match"
                         elif .actual == "allow" then "false_negative" else "false_positive" end)})' \
    "$E09_DIR/zt-matrix.csv" >"$_dir/raw/zt-joined.json" || return 1
  jq -r '["cell", "identity", "broker", "channel", "operation", "design", "expected", "actual", "verdict", "layer", "class", "error"],
    (.[] | [.cell, .identity, .broker, .channel, .operation, .design, .expected, .actual, .verdict, .layer, .class, .error]) | @csv' \
    "$_dir/raw/zt-joined.json" >"$_dir/zt_matrix_result.csv"
  cp "$E09_DIR/zt-matrix.csv" "$_dir/zt-matrix.csv"

  # rbac denied en los sidecars de los brokers (antes y después)
  {
    echo "# contadores rbac...denied de los sidecars (antes de aplicar las políticas / después de la matriz y el flujo)"
    echo "## antes"
    cat "$_dir/raw/rbac-before.txt"
    echo "## después"
    cat "$_dir/raw/rbac-after.txt"
  } >"$_dir/rbac_denied.txt"
  _rk=$(( $(e09_rbac_sum after voltera-infra/kafka-0) - $(e09_rbac_sum before voltera-infra/kafka-0) ))
  _rr=$(( $(e09_rbac_sum after voltera-infra/rabbitmq-0) - $(e09_rbac_sum before voltera-infra/rabbitmq-0) ))
  _rn=$(( $(e09_rbac_sum after voltera-r1/nats) + $(e09_rbac_sum after voltera-r2/nats) + $(e09_rbac_sum after voltera-global/nats) \
        - $(e09_rbac_sum before voltera-r1/nats) - $(e09_rbac_sum before voltera-r2/nats) - $(e09_rbac_sum before voltera-global/nats) ))

  _m_f1=$(jq -n --rawfile m "$_dir/raw/matches-f-r1.psv" '$m | split("\n") | map(select(length > 0)) | length')
  _m_f2=$(jq -n --rawfile m "$_dir/raw/matches-f-r2.psv" '$m | split("\n") | map(select(length > 0)) | length')
  _miss_f1=$(mesh_missing "$_dir/raw/matches-f-r1.psv" "$_dir/raw/ledger.json")
  _miss_f2=$(mesh_missing "$_dir/raw/matches-f-r2.psv" "$_dir/raw/ledger.json")

  jq -n --slurpfile J "$_dir/raw/zt-joined.json" --slurpfile C "$_dir/raw/zt-control.json" \
    --argjson rk "$_rk" --argjson rr "$_rr" --argjson rn "$_rn" \
    --argjson f1 "$_m_f1" --argjson f2 "$_m_f2" --argjson mf1 "$_miss_f1" --argjson mf2 "$_miss_f2" \
    --argjson devmiss "${E09_DEV_MISSING:--1}" --argjson left "${E09_POLICIES_LEFT:--1}" --argjson before "${E09_POLICIES_BEFORE:-0}" \
    --arg run "$E09_RUN" --arg on "$E09_T_ON" --arg matrix "$E09_T_MATRIX" '
    $J[0] as $j
    | ($j | map(select(.design != .expected))) as $gap
    | ($j | map(select(.design == "allow"))) as $pos
    | ($C[0] | map(select(.id == "thief-rabbit.rabbitmq.meter-declarations.publish" and .outcome == "allowed")) | length) as $ctl_rabbit
    | ($C[0] | map(select(.id == "thief-nats.nats.energy.matched.r1.>.publish" and .outcome == "allowed")) | length) as $ctl_nats
    | {run: $run, policies_on: $on, matrix_done: $matrix,
       cells: ($j | length),
       missing: ($j | map(select(.verdict == "missing")) | length),
       matched: ($j | map(select(.verdict == "match")) | length),
       false_negatives: ($j | map(select(.verdict == "false_negative")) | length),
       false_positives: ($j | map(select(.verdict == "false_positive")) | length),
       mismatches_vs_expected: ($j | map(select(.verdict != "match")) | length),
       by_layer: ($j | group_by(.layer) | map({key: .[0].layer, value: {cells: length, matched: (map(select(.verdict == "match")) | length), classes: (map(.class) | group_by(.) | map({key: (.[0] | if . == "" then "allowed" else . end), value: length}) | from_entries)}}) | from_entries),
       by_broker: ($j | group_by(.broker) | map({key: .[0].broker, value: {cells: length, matched: (map(select(.verdict == "match")) | length)}}) | from_entries),
       positives: {cells: ($pos | length), allowed: ($pos | map(select(.actual == "allow")) | length)},
       plain: {cells: ($j | map(select(.identity == "plain")) | length), successes: ($j | map(select(.identity == "plain" and .actual == "allow")) | length)},
       design_gap: {cells: ($gap | length), behaved_as_predicted: ($gap | map(select(.actual == .expected)) | length),
                    cells_list: ($gap | map(.cell)), by_broker: ($gap | group_by(.broker) | map({key: .[0].broker, value: length}) | from_entries)},
       control_without_policy: {thief_rabbit_publish_allowed: $ctl_rabbit, thief_nats_publish_allowed: $ctl_nats},
       rbac_denied_delta: {kafka: $rk, rabbitmq: $rr, nats: $rn},
       flow: {r1_matches: $f1, r2_matches: $f2, r1_missing_in_billing: $mf1, r2_missing_in_billing: $mf2, missing_in_device: $devmiss},
       policies: {before_run: $before, left_after_run: $left}}' >"$_dir/metrics.json" || return 1

  k get authorizationpolicy -A -o yaml >"$_dir/logs/authorizationpolicies.yaml" 2>&1 || true
  for _n in r1 r2 global; do
    k -n "voltera-$_n" logs deploy/nats -c nats --tail=200 >"$_dir/logs/nats-server-$_n.log" 2>&1 || true
  done
  k -n voltera-infra logs rabbitmq-0 -c rabbitmq --tail=300 >"$_dir/logs/rabbitmq.log" 2>&1 || true
}
