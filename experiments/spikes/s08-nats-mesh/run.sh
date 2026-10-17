#!/bin/sh
# S8: event mesh NATS entre dos regiones con autorización. Imprime PASS o FAIL.
# shellcheck disable=SC2329  # los checks se invocan de forma indirecta con check/retry
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/../../lib/common.sh"

need kubectl
need jq
need git
need docker
need mise
need perl
require_cluster

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
out="$ROOT/experiments/results/s08/$(date -u +%Y%m%dT%H%M%SZ)-$sha"
mkdir -p "$out/checks"
log "evidencia en ${out#"$ROOT"/}"

RUN=$(date -u +%d%H%M%S)   # prefijo único por ejecución: los Nats-Msg-Id no chocan con corridas previas
STREAM=BILLING_MATCHED
URL_R1=nats://nats.voltera-r1.svc.cluster.local:4222
URL_R2=nats://nats.voltera-r2.svc.cluster.local:4222
URL_GLOBAL=nats://nats.voltera-global.svc.cluster.local:4222
NS=s08-mesh
n=0
failures=0
: >"$out/result.txt"

check() {
  desc=$1
  shift
  n=$((n + 1))
  if "$@" >"$out/checks/$n.log" 2>&1; then
    line="PASS  $desc"
  else
    line="FAIL  $desc (ver checks/$n.log)"
    failures=$((failures + 1))
  fi
  echo "$line" | tee -a "$out/result.txt"
}

retry() {
  limit=$1
  shift
  elapsed=0
  until "$@"; do
    [ "$elapsed" -lt "$limit" ] || return 1
    sleep 1
    elapsed=$((elapsed + 1))
  done
}

# milisegundos desde la época (date +%s%N no es portable; perl está en Linux y macOS)
now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

cleanup() {
  k -n voltera-global delete authorizationpolicy s08-deny-r2-leaf --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers de cluster ----------------------------------------------------

secret_of() { k -n "$NS" get secret mesh-creds -o json | jq -r --arg u "$1" '.data[$u] | @base64d'; }

# tb <comando...>: ejecuta en la caja de herramientas con sidecar
tb() { k -n "$NS" exec deploy/toolbox -c toolbox -- "$@"; }

# nats_admin <args...>: CLI nats como admin contra el nodo global
nats_admin() {
  tb env NATS_URL="$URL_GLOBAL" NATS_USER=admin NATS_PASSWORD="$(secret_of admin)" nats "$@"
}

mon() { tb curl -fsS --max-time 10 "http://nats.voltera-$1:8222/$2"; }

stream_msgs() { nats_admin stream info "$STREAM" --json | jq '.state.messages'; }

# probe_start <job> <identidad> <url> <args...>: lanza el cliente de prueba como Job con sidecar
probe_start() {
  pname=$1 puser=$2 purl=$3
  shift 3
  pargs=$(jq -nc '$ARGS.positional' --args -- "$@")
  k -n "$NS" delete job "$pname" --ignore-not-found --wait=true >/dev/null
  k apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $pname
  namespace: $NS
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: {app: mesh-probe}
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: voltera/mesh-probe:dev
          imagePullPolicy: IfNotPresent
          args: $pargs
          env:
            - {name: NATS_URL, value: "$purl"}
            - {name: NATS_USER, value: "$puser"}
            - name: NATS_PASSWORD
              valueFrom: {secretKeyRef: {name: mesh-creds, key: "$puser"}}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {memory: 64Mi}
YAML
}

# probe_wait <job>: espera el fin del Job y deja .result en $out/probe-<job>.json
probe_wait() {
  pname=$1
  i=0
  while :; do
    st=$(k -n "$NS" get job "$pname" -o json | jq -r '"\(.status.succeeded // 0)/\(.status.failed // 0)"')
    case "$st" in
      1/*) break ;;
      */0) ;;
      *) break ;;
    esac
    i=$((i + 1))
    [ "$i" -lt 180 ] || { echo "timeout esperando $pname" >&2; return 1; }
    sleep 1
  done
  k -n "$NS" logs "job/$pname" -c probe >"$out/probe-$pname.log" 2>&1 || true
  jq -c 'select(.msg == "result") | .result' "$out/probe-$pname.log" >"$out/probe-$pname.json"
  [ -s "$out/probe-$pname.json" ] || { cat "$out/probe-$pname.log" >&2; return 1; }
}

# run_probe <job> <identidad> <url> <args...>
run_probe() {
  probe_start "$@"
  probe_wait "$1"
}

# pj <job> <filtro jq>: evalúa un filtro sobre el resultado del Job
pj() { jq -e "$2" "$out/probe-$1.json"; }

# ---- despliegue ------------------------------------------------------------

log "S8: despliegue"
mise run mesh:up >"$out/mesh-up.log" 2>&1 || { cat "$out/mesh-up.log" >&2; die "mesh:up falló"; }
mise run images:build mesh-probe >"$out/images-build.log" 2>&1 || { cat "$out/images-build.log" >&2; die "images:build falló"; }
k apply -k "$HERE" >"$out/apply.log"
k -n "$NS" rollout status deploy/toolbox --timeout=120s >/dev/null
k -n s08-plain rollout status deploy/toolbox-plain --timeout=120s >/dev/null
# partida limpia: el stream arranca vacío (el durable conserva su definición)
nats_admin stream purge "$STREAM" --force >"$out/purge.log" 2>&1

# ---- a. enlace entre nodos -------------------------------------------------

leaf_links() {
  mon global leafz >"$out/leafz-global.json"
  mon r1 leafz >"$out/leafz-r1.json"
  mon r2 leafz >"$out/leafz-r2.json"
  jq -e '.leafnodes == 2 and ([.leafs[].name] | sort == ["nats-r1", "nats-r2"])' "$out/leafz-global.json" \
    && jq -e '.leafnodes == 1 and .leafs[0].name == "nats-global" and .leafs[0].is_spoke' "$out/leafz-r1.json" \
    && jq -e '.leafnodes == 1 and .leafs[0].name == "nats-global" and .leafs[0].is_spoke' "$out/leafz-r2.json"
}

# NATS es server-first: con puerto TCP explícito el INFO llega a través del sidecar
server_first_through_sidecar() {
  for t in r1:4222 r2:4222 global:4222 global:7422; do
    node=${t%%:*}
    port=${t##*:}
    # curl no habla NATS: lee el INFO y espera hasta --max-time (código 28)
    tb curl -sS --max-time 3 "telnet://nats.voltera-$node:$port" 2>&1 | grep -q '^INFO {' \
      || { echo "sin INFO en $t"; return 1; }
    echo "INFO recibido de $t"
  done
}

log "S8: a. enlace y protocolo server-first"
check "a. cada región tiene su leafnode activo hacia nats-global (leafz guardado)" retry 30 leaf_links
check "a. INFO server-first llega a través del sidecar (4222 en los 3 nodos y 7422)" server_first_through_sidecar

# ---- b. 100 + 100 con ACK durable ------------------------------------------

log "S8: b. publicación desde ambas regiones"
probe_start pub-r1 bridge-r1 "$URL_R1" publish --subject energy.matched.r1.x --count 100 --msg-id-prefix "$RUN-r1"
probe_start pub-r2 bridge-r2 "$URL_R2" publish --subject energy.matched.r2.x --count 100 --msg-id-prefix "$RUN-r2"
probe_wait pub-r1
probe_wait pub-r2

check "b. bridge-r1 publica 100 desde R1 con 100 PubAck del stream global y 0 errores" \
  pj pub-r1 ".acks == 100 and .errors == 0 and .duplicate_acks == 0 and .streams == [\"$STREAM\"]"
check "b. bridge-r2 publica 100 desde R2 con 100 PubAck del stream global y 0 errores" \
  pj pub-r2 ".acks == 100 and .errors == 0 and .duplicate_acks == 0 and .streams == [\"$STREAM\"]"

run_probe sub-billing billing "$URL_GLOBAL" subscribe --durable billing --expect 200 --timeout 60s
check "b. billing recibe 200 distintos (100 por región) desde el nodo global" \
  pj sub-billing '.received == 200 and .distinct == 200 and .duplicates == 0 and .by_prefix["energy.matched.r1"] == 100 and .by_prefix["energy.matched.r2"] == 100'
check "b. el stream guarda 200 mensajes" test "$(stream_msgs)" -eq 200

# ---- c. autorización ---------------------------------------------------------

log "S8: c. autorización"
probe_start cross-r2 bridge-r2 "$URL_R2" publish --subject energy.matched.r1.x --count 3 --msg-id-prefix "$RUN-cross" --ack-timeout 2s
probe_start rogue-pub rogue "$URL_R1" publish --subject energy.matched.r1.x --count 3 --msg-id-prefix "$RUN-rogue" --ack-timeout 2s
probe_start rogue-sub rogue "$URL_R1" subscribe --core --subject 'energy.matched.>' --timeout 5s
probe_start rogue-dur rogue "$URL_GLOBAL" subscribe --durable billing --timeout 5s
probe_start billing-pub billing "$URL_GLOBAL" publish --subject energy.matched.r1.x --count 3 --msg-id-prefix "$RUN-billing" --ack-timeout 2s
for j in cross-r2 rogue-pub rogue-sub rogue-dur billing-pub; do probe_wait "$j"; done

check "c. bridge-r2 publicando en energy.matched.r1.x: 0 ACK y Permissions Violation for Publish" \
  pj cross-r2 '.acks == 0 and .errors == 3 and .violations >= 1 and (.violation_msgs | any(contains("Permissions Violation for Publish")))'
check "c. rogue no puede publicar (0 ACK, violation)" \
  pj rogue-pub '.acks == 0 and .violations >= 1 and (.violation_msgs | any(contains("Permissions Violation for Publish")))'
check "c. rogue no puede suscribirse (violation, 0 mensajes)" \
  pj rogue-sub '.received == 0 and .violations >= 1 and (.violation_msgs | any(contains("Permissions Violation for Subscription")))'
check "c. rogue no puede usar el durable billing" \
  pj rogue-dur '.received == 0 and (.first_error != null)'
check "c. billing no puede publicar (0 ACK, violation)" \
  pj billing-pub '.acks == 0 and .violations >= 1'

run_probe sub-after-c billing "$URL_GLOBAL" subscribe --durable billing --timeout 5s
msgs_after_c=$(stream_msgs)
check "c. tras los intentos prohibidos billing recibe 0 mensajes nuevos" pj sub-after-c '.received == 0'
check "c. tras los intentos prohibidos el stream sigue en 200" test "$msgs_after_c" -eq 200

# ---- d. duplicado ------------------------------------------------------------

log "S8: d. duplicado por Nats-Msg-Id"
before_d=$(stream_msgs)
run_probe dup bridge-r1 "$URL_R1" publish --subject energy.matched.r1.dup --count 5 --msg-id-prefix "$RUN-dup" --repeat 2
after_d=$(stream_msgs)
check "d. mismo Nats-Msg-Id publicado 2 veces: 10 ACK (5 marcados duplicate)" \
  pj dup '.acks == 10 and .duplicate_acks == 5 and .errors == 0'
check "d. el stream guarda 1 copia por id (+5 mensajes, no +10)" test "$((after_d - before_d))" -eq 5

# ---- e. partición R2 <-> global -------------------------------------------

log "S8: e. partición R2 <-> global"
base_e=$(stream_msgs)
POLICY='apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata:
  name: s08-deny-r2-leaf
  namespace: voltera-global
spec:
  selector:
    matchLabels: {app: nats}
  action: DENY
  rules:
    - from:
        - source:
            principals: ["cluster.local/ns/voltera-r2/sa/nats"]
      to:
        - operation:
            ports: ["7422"]'

global_leafs() { mon global leafz | jq -r '.leafnodes'; }
r2_leafs() { mon r2 leafz | jq -r '.leafnodes'; }
leafs_eq() { [ "$($1)" -eq "$2" ]; }

t_cut=$(now_ms)
echo "$POLICY" | k apply -f - >/dev/null
# DENY L4 solo afecta conexiones nuevas: se reinicia el nodo R2 para tirar el enlace ya establecido
k -n voltera-r2 delete pod -l app=nats --wait=false >/dev/null
retry 60 leafs_eq global_leafs 1 || true
t_down=$(now_ms)
k -n voltera-r2 rollout status deploy/nats --timeout=120s >/dev/null
sleep 8   # margen para que el leaf de R2 intente (y falle) reconectar
mon global leafz >"$out/leafz-global-partition.json"
mon r2 leafz >"$out/leafz-r2-partition.json"

check "e. con la partición activa nats-global solo ve el leafnode de R1 y R2 no tiene enlace" \
  sh -c "jq -e '.leafnodes == 1 and .leafs[0].name == \"nats-r1\"' '$out/leafz-global-partition.json' && jq -e '.leafnodes == 0' '$out/leafz-r2-partition.json'"

probe_start part-r2 bridge-r2 "$URL_R2" publish --subject energy.matched.r2.x --count 20 --msg-id-prefix "$RUN-part-r2" --ack-timeout 2s
probe_start part-r1 bridge-r1 "$URL_R1" publish --subject energy.matched.r1.x --count 20 --msg-id-prefix "$RUN-part-r1" --ack-timeout 2s
probe_wait part-r2
probe_wait part-r1
check "e. publicar desde R2 sin enlace falla sin ACK (0 ACK, 20 errores: sin falsa confirmación)" \
  pj part-r2 '.acks == 0 and .errors == 20'
check "e. R1 sigue publicando con ACK durante la partición de R2" \
  pj part-r1 '.acks == 20 and .errors == 0'
check "e. el stream solo guardó los 20 de R1 durante la partición" test "$(stream_msgs)" -eq "$((base_e + 20))"

# el bridge reintenta los mismos ids mientras dura la partición y obtiene ACK al volver el enlace
probe_start heal-r2 bridge-r2 "$URL_R2" publish --subject energy.matched.r2.x --count 20 --msg-id-prefix "$RUN-part-r2" --ack-timeout 2s --retry-timeout 90s
k -n "$NS" wait --for=condition=Ready pod -l job-name=heal-r2 --timeout=60s >/dev/null
sleep 3   # el publicador ya está reintentando contra el enlace caído
t_restore=$(now_ms)
k -n voltera-global delete authorizationpolicy s08-deny-r2-leaf >/dev/null
retry 60 leafs_eq r2_leafs 1 || true
t_link=$(now_ms)
probe_wait heal-r2
t_healed=$(now_ms)
retry 60 leafs_eq global_leafs 2 || true
mon global leafz >"$out/leafz-global-healed.json"

check "e. restaurado el enlace, el reintento de los mismos 20 ids obtiene 20 ACK (con reintentos durante la partición)" \
  pj heal-r2 '.acks == 20 and .errors == 0 and .retries > 0'
check "e. el stream tiene 40 nuevos (20 R1 + 20 R2) sin duplicados" test "$(stream_msgs)" -eq "$((base_e + 40))"
run_probe sub-after-e billing "$URL_GLOBAL" subscribe --durable billing --expect 45 --timeout 60s
check "e. billing recibe los 45 nuevos (5 dedupe + 40 de la partición), todos distintos" \
  pj sub-after-e '.received == 45 and .distinct == 45'

jq -n --argjson cut "$t_cut" --argjson down "$t_down" --argjson restore "$t_restore" \
  --argjson link "$t_link" --argjson healed "$t_healed" \
  '{cut_to_link_down_ms: ($down - $cut), restore_to_link_up_ms: ($link - $restore),
    restore_to_all_acked_ms: ($healed - $restore), note: "un solo host; incluye la latencia de kubectl exec en la medición"}' \
  >"$out/partition-times.json"
cat "$out/partition-times.json"

# ---- f. pod sin sidecar -----------------------------------------------------

log "S8: f. pod sin sidecar"
plain() { k -n s08-plain exec deploy/toolbox-plain -c toolbox -- "$@"; }

plain_has_network() { plain curl -fsS --max-time 10 http://prometheus.istio-system:9090/-/ready; }

plain_rejected() {
  for t in r1:4222 r2:4222 global:4222 global:7422 global:8222; do
    node=${t%%:*}
    port=${t##*:}
    if plain curl -sS --max-time 5 "telnet://nats.voltera-$node:$port" >"$out/plain-$node-$port.txt" 2>&1; then
      rc=0
    else
      rc=$?
    fi
    if grep -q 'INFO {' "$out/plain-$node-$port.txt"; then
      echo "el pod sin sidecar recibió INFO de $t"
      return 1
    fi
    [ "$rc" -ne 0 ] || { echo "conexión aceptada en $t"; return 1; }
    echo "$t rechazado (curl rc=$rc)"
  done
}

# `nats rtt` sale con 0 aunque falle: el resultado se lee de su salida ("failed")
rtt_as_bridge_r1() {
  pw=$(secret_of bridge-r1)
  "$@" env NATS_URL="$URL_R1" NATS_USER=bridge-r1 NATS_PASSWORD="$pw" nats rtt --timeout 5s
}

# credenciales válidas: el rechazo es de la capa de transporte (mTLS), no de autenticación
plain_nats_client_rejected() {
  rtt_as_bridge_r1 plain >"$out/plain-nats-rtt.txt" 2>&1 || true
  cat "$out/plain-nats-rtt.txt"
  grep -q failed "$out/plain-nats-rtt.txt"
}

mesh_nats_client_ok() {
  rtt_as_bridge_r1 tb >"$out/mesh-nats-rtt.txt" 2>&1
  cat "$out/mesh-nats-rtt.txt"
  ! grep -q failed "$out/mesh-nats-rtt.txt"
}

check "f. control positivo: el pod sin sidecar tiene red (llega a Prometheus)" plain_has_network
check "f. control positivo: el mismo cliente NATS con sidecar y credenciales válidas conecta" mesh_nats_client_ok
check "f. pod sin sidecar no recibe INFO ni conecta a 4222/7422/8222 de los nodos (mTLS STRICT)" plain_rejected
check "f. cliente NATS sin sidecar con credenciales válidas no conecta" plain_nats_client_rejected

# ---- evidencia y recursos ---------------------------------------------------

log "S8: evidencia"
nats_admin stream info "$STREAM" --json >"$out/stream-info.json" 2>&1 || true
nats_admin consumer info "$STREAM" billing --json >"$out/consumer-info.json" 2>&1 || true
for node in global r1 r2; do mon "$node" varz >"$out/varz-$node.json" 2>&1 || true; done
k get pods -A -o wide | grep -E 'NAMESPACE|nats' >"$out/pods.txt" || true
sleep 20   # metrics-server refresca cada ~15 s
k top pods -A --containers | grep -E 'NAMESPACE|nats' >"$out/top-nats.txt" || true
cat "$out/top-nats.txt"

if [ "$failures" -eq 0 ]; then
  k delete namespace s08-mesh s08-plain --wait=false >/dev/null
  echo "PASS" | tee -a "$out/result.txt"
  exit 0
fi
echo "FAIL ($failures checks fallidos de $n)" | tee -a "$out/result.txt"
exit 1
