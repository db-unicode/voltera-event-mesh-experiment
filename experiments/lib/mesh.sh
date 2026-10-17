#!/bin/sh
# Helpers de E8, E9 y E10 (event mesh, Zero Trust del bus, traza de un evento). Se usa con `.`; no se ejecuta.
# Requiere haber cargado lib/common.sh (k, log, die), lib/expctl.sh (expctl) y lib/metersim.sh (metersim_run).
#
#   mesh_tools_up / mesh_tools_down          toolbox con sidecar (curl, jq, nats) en el namespace bus-tools
#   tb <cmd...>                              ejecuta en la toolbox
#   mesh_monitor <r1|r2|global> <ruta>       GET al puerto de monitoreo (8222) del nodo NATS, vía toolbox
#   mesh_leafs <nodo>                        número de leafnodes conectados (.leafnodes de /leafz)
#   mesh_stream_seq / mesh_consumer_pending  last_seq del stream BILLING_MATCHED / mensajes pendientes del durable
#   mesh_ledger <archivo>                    GET /state?detail=1 de billing-sim
#   mesh_pg <r1|r2> <statefulset> <sql>      psql -At en positions-db / matches-db / transfer-db
#   mesh_pairs <región> <prefijo> <rate> <s> <archivo>   meter-sim `pairs` (rate = declaraciones/s = 2 x pares/s)
#   mesh_wait_deploy <ns> <deploy> [s]       espera a que el Deployment tenga todas las réplicas listas
#   mesh_retry <s> <cmd...>                  reintenta cada 2 s hasta que el comando pase
#   mesh_epoch <RFC3339>                     segundos enteros desde la época
#   mesh_matches <región> <prefijo> <arch>   match_id|epoch de los matches con ese prefijo de medidor (matches-db)
#   mesh_settle <s> <tag=región=prefijo>...  espera a que billing tenga todos esos matches y dejen de aparecer; deja
#                                            $OUT/raw/ledger.json y $OUT/raw/matches-<tag>.psv
#   mesh_counter <ns> <app> <métrica>        suma de una métrica en los pods de la app (0 si no existe)

MESH_TOOLS_NS=${MESH_TOOLS_NS:-bus-tools}
MESH_PAIR_METERS=${MESH_PAIR_METERS:-400}

mesh_tools_up() {
  k apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $MESH_TOOLS_NS
  labels: {istio-injection: enabled}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: toolbox
  namespace: $MESH_TOOLS_NS
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: toolbox
  namespace: $MESH_TOOLS_NS
spec:
  replicas: 1
  selector:
    matchLabels: {app: toolbox}
  template:
    metadata:
      labels: {app: toolbox}
    spec:
      serviceAccountName: toolbox
      containers:
        - name: toolbox
          image: natsio/nats-box:0.20.0-nonroot
          imagePullPolicy: IfNotPresent
          command: ["sleep", "infinity"]
          resources:
            requests: {cpu: 5m, memory: 8Mi}
            limits: {memory: 64Mi}
YAML
  k -n "$MESH_TOOLS_NS" rollout status deploy/toolbox --timeout=180s >/dev/null
}

mesh_tools_down() { k delete namespace "$MESH_TOOLS_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }

tb() { k -n "$MESH_TOOLS_NS" exec deploy/toolbox -c toolbox -- "$@"; }

mesh_monitor() { tb curl -fsS --max-time 10 "http://nats.voltera-$1:8222/$2"; }

mesh_leafs() { mesh_monitor "$1" leafz | jq -r '.leafnodes'; }

mesh_stream_seq() {
  mesh_monitor global 'jsz?streams=true' \
    | jq -r '[.account_details[]?.stream_detail[]? | select(.name == "BILLING_MATCHED") | .state.last_seq][0] // empty'
}

mesh_consumer_pending() {
  mesh_monitor global 'jsz?consumers=true' \
    | jq -r '[.account_details[]?.stream_detail[]? | select(.name == "BILLING_MATCHED") | .consumer_detail[]? | select(.name == "billing") | (.num_pending + .num_ack_pending)][0] // empty'
}

mesh_ledger() { tb curl -fsS --max-time 20 "http://billing-sim.voltera-global:8080/state?detail=1" >"$1"; }

mesh_pg() { k -n "voltera-$1" exec "statefulset/$2" -c postgres -- psql -U voltera -d voltera -Atc "$3"; }

# mesh_pairs <región> <prefijo> <rate> <segundos> <archivo>: guarda el log y deja el resumen en <archivo>.summary.json
mesh_pairs() {
  _mp_region=$1 _mp_prefix=$2 _mp_rate=$3 _mp_dur=$4 _mp_file=$5
  metersim_run "$_mp_region" --profile pairs --rate "$_mp_rate" --duration "${_mp_dur}s" --meters "$MESH_PAIR_METERS" \
    --zones 4 --prefix "$_mp_prefix" >"$_mp_file" 2>&1 || return 1
  metersim_summary <"$_mp_file" >"$_mp_file.summary.json"
  jq -e '.published > 0' "$_mp_file.summary.json" >/dev/null
}

mesh_wait_deploy() {
  _mw_limit=${3:-180}
  k -n "$1" rollout status "deploy/$2" --timeout="${_mw_limit}s" >/dev/null
}

# ---- sondas Zero Trust (zt-probe) ------------------------------------------------------------------------------
#   mesh_sa_up <ns> <sa>                     crea el ServiceAccount si no existe
#   mesh_probe_job <ns> <nombre> <sa> <celdas.csv> [VAR=valor | VAR=secret:<secret>:<clave>]...
#                                            ConfigMap con las celdas + Job de voltera/zt-probe:dev con esa identidad
#   mesh_probe_wait <ns> <nombre> <salida.jsonl> [s]   espera al Job, guarda sus resultados (un JSON por celda)
#   mesh_probe_cleanup <ns> <nombre>         borra Job y ConfigMap

mesh_sa_up() { k -n "$1" get sa "$2" >/dev/null 2>&1 || k -n "$1" create sa "$2" >/dev/null; }

mesh_probe_job() {
  _pj_ns=$1 _pj_name=$2 _pj_sa=$3 _pj_cells=$4
  shift 4
  k -n "$_pj_ns" delete job "$_pj_name" --ignore-not-found --wait=true >/dev/null
  k -n "$_pj_ns" delete configmap "$_pj_name-cells" --ignore-not-found >/dev/null
  k -n "$_pj_ns" create configmap "$_pj_name-cells" --from-file=cells.csv="$_pj_cells" >/dev/null
  _pj_env=
  for _pj_kv in "$@"; do
    _pj_key=${_pj_kv%%=*}
    _pj_val=${_pj_kv#*=}
    case $_pj_val in
      secret:*)
        _pj_rest=${_pj_val#secret:}
        _pj_env="$_pj_env
            - name: $_pj_key
              valueFrom: {secretKeyRef: {name: ${_pj_rest%%:*}, key: ${_pj_rest#*:}}}"
        ;;
      *)
        _pj_env="$_pj_env
            - {name: $_pj_key, value: \"$_pj_val\"}"
        ;;
    esac
  done
  k -n "$_pj_ns" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $_pj_name
  labels: {app: zt-probe-job}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 300
  ttlSecondsAfterFinished: 900
  template:
    metadata:
      labels: {app: zt-probe-job}
    spec:
      serviceAccountName: $_pj_sa
      restartPolicy: Never
      containers:
        - name: zt-probe
          image: voltera/zt-probe:dev
          imagePullPolicy: IfNotPresent
          args: ["--cells", "/cells/cells.csv", "--timeout", "8s", "--parallel", "6"]
          env:
            - {name: ZT_IDENTITY, value: "$_pj_name"}$_pj_env
          volumeMounts:
            - {name: cells, mountPath: /cells, readOnly: true}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {memory: 96Mi}
      volumes:
        - name: cells
          configMap: {name: $_pj_name-cells}
YAML
}

mesh_probe_wait() {
  _pw_ns=$1 _pw_name=$2 _pw_out=$3 _pw_limit=${4:-300}
  _pw_i=0
  while :; do
    _pw_st=$(k -n "$_pw_ns" get job "$_pw_name" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
    case $_pw_st in
      1/*) break ;;
      */1) break ;;
    esac
    _pw_i=$((_pw_i + 2))
    [ "$_pw_i" -lt "$_pw_limit" ] || { log "timeout esperando el Job $_pw_ns/$_pw_name"; return 1; }
    sleep 2
  done
  k -n "$_pw_ns" logs "job/$_pw_name" -c zt-probe >"$_pw_out" 2>&1 || true
  case $_pw_st in 1/*) return 0 ;; esac
  log "el Job $_pw_ns/$_pw_name falló"
  return 1
}

mesh_probe_cleanup() {
  k -n "$1" delete job "$2" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  k -n "$1" delete configmap "$2-cells" --ignore-not-found >/dev/null 2>&1 || true
}

mesh_retry() {
  _r_limit=$1
  shift
  _r_el=0
  until "$@"; do
    [ "$_r_el" -lt "$_r_limit" ] || return 1
    sleep 2
    _r_el=$((_r_el + 2))
  done
}

mesh_epoch() { printf '%s' "$1" | jq -Rr 'sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601'; }

mesh_matches() {
  mesh_pg "$1" matches-db "select match_id || '|' || extract(epoch from created_at) from matches where seller_meter_id like '$2%' or buyer_meter_id like '$2%' order by created_at" >"$3"
}

# matchIds de <archivo de matches> que billing (<ledger>) aún no tiene
mesh_missing() {
  jq -r --rawfile m "$1" '. as $r | ($m | split("\n") | map(select(length > 0) | split("|")[0]))
    | map(select(($r.byMatchId[.].count // 0) < 1)) | length' "$2"
}

mesh_settle() {
  _st_limit=$1
  shift
  _st_el=0
  _st_prev=-1
  _st_stable=0
  while :; do
    mesh_ledger "$OUT/raw/ledger.json" || true
    _st_missing=0
    _st_total=0
    for _st_spec in "$@"; do
      _st_tag=${_st_spec%%=*}
      _st_rest=${_st_spec#*=}
      mesh_matches "${_st_rest%%=*}" "${_st_rest#*=}" "$OUT/raw/matches-$_st_tag.psv" || true
      _st_n=$(grep -c . "$OUT/raw/matches-$_st_tag.psv" || true)
      _st_total=$((_st_total + _st_n))
      _st_miss=$(mesh_missing "$OUT/raw/matches-$_st_tag.psv" "$OUT/raw/ledger.json" 2>/dev/null || echo 999999)
      _st_missing=$((_st_missing + _st_miss))
    done
    if [ "$_st_missing" -eq 0 ] && [ "$_st_total" -gt 0 ] && [ "$_st_total" -eq "$_st_prev" ]; then
      _st_stable=$((_st_stable + 1))
    else
      _st_stable=0
    fi
    _st_prev=$_st_total
    [ "$_st_stable" -lt 2 ] || return 0
    [ "$_st_el" -lt "$_st_limit" ] || { log "settle: faltan $_st_missing de $_st_total matches tras ${_st_limit}s"; return 1; }
    sleep 4
    _st_el=$((_st_el + 4))
  done
}

mesh_counter() {
  _c_sum=0
  for _c_pod in $(k -n "$1" get pods -l "app=$2" -o name | sed 's|^pod/||'); do
    _c_v=$(expctl http-get "$1/pod/$_c_pod:9090/metrics" 2>/dev/null | awk -v n="$3" '$1 ~ ("^" n "({|$)") { s += $NF } END { printf "%d\n", s }')
    _c_sum=$((_c_sum + ${_c_v:-0}))
  done
  echo "$_c_sum"
}
