#!/bin/sh
# Helpers compartidos de E5 (duplicados) y E6 (outbox). Se usan con `.` desde el run.sh de cada
# experimento; no se ejecuta. Requiere el runner (k, log, die, expctl, chaos_*, wait_ready, OUT).
#
#   e56_bg <cmd...>                   lanza en segundo plano; pid en E56_LAST (e56_stop_bg mata todos)
#   e56_kill <pid>                    mata un proceso de e56_bg y sus hijos
#   e56_pf <ns> <target> <loc> <rem>  port-forward que se relanza solo si cae
#   e56_wait <segundos> <cmd...>      reintenta cada 2 s hasta que el comando pase
#   e56_psql <ns> <sts> <sql>         salida plana de psql (-tA) contra una base del cluster
#   e56_metric_sum <ns> <deploy> <métrica> [label="valor"...]   suma de la serie en /metrics de todos los pods
#   e56_pod_restarts <ns> <deploy>    reinicios acumulados del contenedor del deployment
#   e56_arm <ns> <deploy> <fp> <modo> arma un failpoint por HTTP en cada pod (sin fallback que reinicie pods)
#   e56_rearm_loop <ns> <deploy> <fp> <modo> <máx> <csv>   arma y re-arma tras cada crash (hasta <máx>); anota crashes
#   e56_keda_pause <ns> <so> <n>      fija réplicas con la anotación (recuerda el valor original)
#   e56_keda_restore                  devuelve los ScaledObject a su estado original
#   mset <clave> <valor> / msets <clave> <texto>   acumulan métricas en $OUT/raw/m.json
#   kafka_job <ns> <nombre> <cm> <script>          Job con apache/kafka (ConfigMap <cm> montado en /in; "-" = sin ConfigMap), espera y devuelve su log

E56_PIDS=""
E56_LAST=""
E56_KEDA_ORIG=""
E56_KAFKA_IMAGE=apache/kafka:4.3.1

e56_bg() {
  "$@" &
  E56_LAST=$!
  E56_PIDS="$E56_PIDS $E56_LAST"
}

e56_stop_bg() {
  for _p in $E56_PIDS; do
    pkill -P "$_p" 2>/dev/null || true
    kill "$_p" 2>/dev/null || true
  done
  E56_PIDS=""
}

e56_kill() {
  pkill -P "$1" 2>/dev/null || true
  kill "$1" 2>/dev/null || true
}

# e56_pf <ns> <target> <local> <remote>
e56_pf() {
  (while :; do
    k -n "$1" port-forward "$2" "$3:$4" >/dev/null 2>&1 || true
    sleep 1
  done) &
  E56_PIDS="$E56_PIDS $!"
}

# e56_wait <segundos> <cmd...>
e56_wait() {
  _limit=$1
  shift
  _e=0
  until "$@"; do
    [ "$_e" -lt "$_limit" ] || return 1
    sleep 2
    _e=$((_e + 2))
  done
}

# e56_psql <ns> <statefulset> <sql>
e56_psql() {
  k -n "$1" exec "$2-0" -c postgres -- psql -U voltera -d voltera -tAc "$3"
}

# e56_metric_sum <ns> <deploy> <métrica> [label="valor" ...]: suma lo que exponen los pods ahora mismo
e56_metric_sum() {
  _ns=$1 _dep=$2 _m=$3
  shift 3
  _sel=$(_chaos_selector "$_ns" "$_dep") || return 1
  _tot=0
  for _pod in $(_chaos_pods "$_ns" "$_sel"); do
    _v=$(expctl http-get "$_ns/pod/$_pod:9090/metrics" | awk -v m="$_m" -v want="$*" '
      BEGIN { n = split(want, w, " ") }
      index($0, m "{") == 1 || index($0, m " ") == 1 {
        ok = 1
        for (i = 1; i <= n; i++) if (index($0, w[i]) == 0) ok = 0
        if (ok) s += $NF
      }
      END { printf "%.0f\n", s }') || return 1
    _tot=$((_tot + _v))
  done
  echo "$_tot"
}

# e56_pod_restarts <ns> <deploy>
e56_pod_restarts() {
  _sel=$(_chaos_selector "$1" "$2") || return 1
  k -n "$1" get pods -l "$_sel" -o json \
    | jq --arg c "$2" '[.items[].status.containerStatuses[]? | select(.name == $c) | .restartCount] | add // 0'
}

# e56_last_crash <ns> <deploy>: instante (UTC) del último fin de contenedor por crash entre los pods
e56_last_crash() {
  _sel=$(_chaos_selector "$1" "$2") || return 1
  k -n "$1" get pods -l "$_sel" -o json \
    | jq -r --arg c "$2" '[.items[].status.containerStatuses[]? | select(.name == $c) | .lastState.terminated.finishedAt // empty] | max // ""'
}

# e56_arm <ns> <deploy> <failpoint> <modo>: POST a /admin/failpoint de cada pod; falla si algún pod no responde.
# No usa el fallback de chaos_failpoint (kubectl set env) porque reiniciaría los pods.
e56_arm() {
  _sel=$(_chaos_selector "$1" "$2") || return 1
  _pods=$(_chaos_pods "$1" "$_sel")
  [ -n "$_pods" ] || return 1
  for _pod in $_pods; do
    expctl http-get --method POST "$1/pod/$_pod:9090/admin/failpoint?name=$3&mode=$4" >/dev/null || return 1
  done
}

# e56_rearm_loop <ns> <deploy> <failpoint> <modo> <máx crashes> <csv>
# Arma el failpoint y, cada vez que el contenedor se reinicia (el crash ya ocurrió), lo vuelve a armar
# hasta <máx> crashes. Escribe ts,deploy,failpoint,restarts en el csv (ts = fin del contenedor caído).
e56_rearm_loop() {
  _ns=$1 _dep=$2 _fp=$3 _mode=$4 _max=$5 _csv=$6
  [ -s "$_csv" ] || echo "ts,deploy,failpoint,restarts" >"$_csv"
  _base=$(e56_pod_restarts "$_ns" "$_dep" || echo 0)
  _seen=$_base
  e56_wait 60 e56_arm "$_ns" "$_dep" "$_fp" "$_mode" || log "no se pudo armar $_fp en $_dep"
  while [ $((_seen - _base)) -lt "$_max" ]; do
    sleep 1
    _now=$(e56_pod_restarts "$_ns" "$_dep" 2>/dev/null || echo "$_seen")
    [ "$_now" -gt "$_seen" ] || continue
    printf '%s,%s,%s,%s\n' "$(e56_last_crash "$_ns" "$_dep")" "$_dep" "$_fp" "$_now" >>"$_csv"
    _seen=$_now
    [ $((_seen - _base)) -lt "$_max" ] || break
    wait_ready "$_ns" "$_dep" 120 || true
    e56_wait 60 e56_arm "$_ns" "$_dep" "$_fp" "$_mode" || log "no se pudo re-armar $_fp en $_dep"
  done
}

# e56_disarm <ns> <deploy> <failpoint...>: modo off en cada pod (los pods reiniciados ya nacen sin failpoints)
e56_disarm() {
  _ns=$1 _dep=$2
  shift 2
  _sel=$(_chaos_selector "$_ns" "$_dep") 2>/dev/null || return 0
  for _pod in $(_chaos_pods "$_ns" "$_sel"); do
    for _fp in "$@"; do
      expctl http-get --method POST "$_ns/pod/$_pod:9090/admin/failpoint?name=$_fp&mode=off" >/dev/null 2>&1 || true
    done
  done
}

# e56_keda_pause <ns> <scaledobject> <n>: fija las réplicas y espera a que estén listas
e56_keda_pause() {
  _orig=$(k -n "$1" get scaledobject "$2" -o json | jq -r '.metadata.annotations["autoscaling.keda.sh/paused-replicas"] // ""')
  E56_KEDA_ORIG="$E56_KEDA_ORIG $1/$2=$_orig"
  chaos_pause_keda "$1" "$2" "$3"
  e56_wait 120 _e56_replicas_are "$1" "$2" "$3" || return 1
  wait_ready "$1" "$2" 180
}

_e56_replicas_are() { [ "$(k -n "$1" get deploy "$2" -o jsonpath='{.status.readyReplicas}')" = "$3" ]; }

e56_keda_restore() {
  for _item in $E56_KEDA_ORIG; do
    _so=${_item%%=*}
    _orig=${_item#*=}
    if [ -n "$_orig" ]; then
      k -n "${_so%%/*}" annotate scaledobject "${_so#*/}" "autoscaling.keda.sh/paused-replicas=$_orig" --overwrite >/dev/null 2>&1 || true
    else
      k -n "${_so%%/*}" annotate scaledobject "${_so#*/}" "autoscaling.keda.sh/paused-replicas-" >/dev/null 2>&1 || true
    fi
  done
  E56_KEDA_ORIG=""
}

# mset <clave> <valor numérico/booleano/null> / msets <clave> <texto>: métricas acumuladas en $OUT/raw/m.json
mset() {
  [ -s "$OUT/raw/m.json" ] || echo '{}' >"$OUT/raw/m.json"
  _v=${2:-null}
  jq --arg k "$1" --argjson v "$_v" '.[$k] = $v' "$OUT/raw/m.json" >"$OUT/raw/m.json.tmp" && mv "$OUT/raw/m.json.tmp" "$OUT/raw/m.json"
}

msets() {
  [ -s "$OUT/raw/m.json" ] || echo '{}' >"$OUT/raw/m.json"
  jq --arg k "$1" --arg v "$2" '.[$k] = $v' "$OUT/raw/m.json" >"$OUT/raw/m.json.tmp" && mv "$OUT/raw/m.json.tmp" "$OUT/raw/m.json"
}

# kafka_job <ns> <nombre> <configmap> <script>: Job con apache/kafka. El ConfigMap (ya creado) se monta en /in.
# Espera a que termine (hasta 300 s), imprime su log por stdout y borra el Job. Devuelve 1 si falló.
kafka_job() {
  _ns=$1 _name=$2 _cm=$3 _script=$4
  k -n "$_ns" delete job "$_name" --ignore-not-found --wait=true >/dev/null
  _scriptjson=$(printf '%s' "$_script" | jq -Rs .)
  _mounts="[]"
  _vols="[]"
  if [ "$_cm" != "-" ]; then
    _mounts='[{name: in, mountPath: /in}]'
    _vols="[{name: in, configMap: {name: $_cm}}]"
  fi
  k -n "$_ns" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $_name
  labels: {app: $_name}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 280
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: {app: $_name}
    spec:
      restartPolicy: Never
      containers:
        - name: kafka
          image: $E56_KAFKA_IMAGE
          command: [sh, -c]
          args: [$_scriptjson]
          env:
            - {name: KAFKA_HEAP_OPTS, value: "-Xms64m -Xmx192m"}
          volumeMounts: $_mounts
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {memory: 384Mi}
      volumes: $_vols
YAML
  _i=0
  _rc=1
  while [ "$_i" -lt 150 ]; do
    _st=$(k -n "$_ns" get job "$_name" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $_st in
      1/*) _rc=0; break ;;
      */1) break ;;
    esac
    _i=$((_i + 1))
    sleep 2
  done
  k -n "$_ns" logs "job/$_name" -c kafka 2>/dev/null || true
  k -n "$_ns" delete job "$_name" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  return "$_rc"
}

# e56_ensure_ns <ns>: namespace de prueba con sidecar
e56_ensure_ns() {
  k apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $1
  labels: {istio-injection: enabled}
YAML
}
