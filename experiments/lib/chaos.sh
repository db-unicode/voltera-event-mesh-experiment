#!/bin/sh
# Primitivas de caos (S12). Se usa con `.`; no se ejecuta.
# Requiere haber cargado lib/common.sh (k, log, die) y lib/expctl.sh (expctl, EXPCTL).
#
#   chaos_kill <ns> <deploy>                 borra los pods del deployment sin gracia
#   chaos_down <ns> <deploy>                 escala a 0 y espera a que no queden pods
#   chaos_up <ns> <deploy> [n]               escala a n (por defecto 1); no espera, usa wait_ready
#   chaos_pause_keda <ns> <so> <n>           fija las réplicas con la anotación paused-replicas
#   chaos_resume_keda <ns> <so>              quita la anotación y KEDA retoma el control
#   chaos_failpoint <ns> <deploy> <n=modo>   POST /admin/failpoint en cada pod; si falla, FAILPOINTS=
#   chaos_partition <ns-from> <ns-to> <port> DENY L4 de ns-from hacia el puerto en ns-to
#   chaos_heal                               elimina todas las particiones creadas por chaos_partition
#   wait_ready <ns> <deploy> [timeout_s]     espera a que todos los pods deseados estén Ready (120 s)
#
# Notas:
# - chaos_down/chaos_up sobre un deployment gobernado por KEDA los pisa el HPA: usa chaos_pause_keda.
# - chaos_partition (AuthorizationPolicy DENY) solo afecta conexiones nuevas en TCP puro; para
#   tirar un enlace ya establecido reinicia al cliente con chaos_kill. En HTTP aplica por petición.
# - <port> es el puerto del contenedor destino, no el del Service.
# - Variables: CHAOS_OPS_PORT (9090) y CHAOS_FAILPOINT_PATH (/admin/failpoint).

CHAOS_OPS_PORT=${CHAOS_OPS_PORT:-9090}
CHAOS_FAILPOINT_PATH=${CHAOS_FAILPOINT_PATH:-/admin/failpoint}

# _chaos_selector <ns> <deploy>: selector de pods como a=b,c=d
_chaos_selector() {
  k -n "$1" get deploy "$2" -o json \
    | jq -er '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")'
}

# _chaos_ready_pods <ns> <selector>: número de pods Ready y sin borrar
_chaos_ready_pods() {
  k -n "$1" get pods -l "$2" -o json \
    | jq '[.items[] | select(.metadata.deletionTimestamp == null)
           | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length'
}

# _chaos_pods <ns> <selector>: nombres de pods no borrados
_chaos_pods() {
  k -n "$1" get pods -l "$2" -o json \
    | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name'
}

chaos_kill() {
  [ "$#" -eq 2 ] || die "uso: chaos_kill <ns> <deploy>"
  sel=$(_chaos_selector "$1" "$2") || die "deployment $1/$2 no existe"
  log "chaos: kill $1/$2"
  k -n "$1" delete pod -l "$sel" --grace-period=0 --force --wait=false >/dev/null 2>&1
}

chaos_down() {
  [ "$#" -eq 2 ] || die "uso: chaos_down <ns> <deploy>"
  log "chaos: down $1/$2"
  k -n "$1" scale deploy "$2" --replicas=0 >/dev/null
  wait_ready "$1" "$2"
}

chaos_up() {
  [ "$#" -ge 2 ] || die "uso: chaos_up <ns> <deploy> [n]"
  log "chaos: up $1/$2 -> ${3:-1}"
  k -n "$1" scale deploy "$2" --replicas="${3:-1}" >/dev/null
}

chaos_pause_keda() {
  [ "$#" -eq 3 ] || die "uso: chaos_pause_keda <ns> <scaledobject> <n>"
  log "chaos: pausa KEDA $1/$2 en $3 réplicas"
  k -n "$1" annotate scaledobject "$2" "autoscaling.keda.sh/paused-replicas=$3" --overwrite >/dev/null
}

chaos_resume_keda() {
  [ "$#" -eq 2 ] || die "uso: chaos_resume_keda <ns> <scaledobject>"
  log "chaos: reanuda KEDA $1/$2"
  k -n "$1" annotate scaledobject "$2" "autoscaling.keda.sh/paused-replicas-" >/dev/null
}

chaos_failpoint() {
  [ "$#" -eq 3 ] || die "uso: chaos_failpoint <ns> <deploy> <name=modo>"
  fp_ns=$1 fp_deploy=$2 fp_spec=$3
  fp_name=${fp_spec%%=*}
  fp_mode=${fp_spec#*=}
  [ "$fp_name" != "$fp_spec" ] && [ -n "$fp_name" ] && [ -n "$fp_mode" ] || die "failpoint inválido: $fp_spec (se espera name=modo)"
  sel=$(_chaos_selector "$fp_ns" "$fp_deploy") || die "deployment $fp_ns/$fp_deploy no existe"
  fp_pods=$(_chaos_pods "$fp_ns" "$sel")
  fp_ok=1
  [ -n "$fp_pods" ] || fp_ok=0
  for pod in $fp_pods; do
    expctl http-get --method POST \
      "$fp_ns/pod/$pod:$CHAOS_OPS_PORT$CHAOS_FAILPOINT_PATH?name=$fp_name&mode=$fp_mode" >/dev/null 2>&1 || fp_ok=0
  done
  if [ "$fp_ok" -eq 1 ]; then
    log "chaos: failpoint $fp_spec en $fp_ns/$fp_deploy (HTTP)"
    return 0
  fi
  log "chaos: failpoint por HTTP falló; fallback kubectl set env FAILPOINTS=$fp_spec (reinicia los pods)"
  k -n "$fp_ns" set env "deploy/$fp_deploy" "FAILPOINTS=$fp_spec" >/dev/null
}

chaos_partition() {
  [ "$#" -eq 3 ] || die "uso: chaos_partition <ns-from> <ns-to> <port>"
  log "chaos: partición $1 -> $2:$3"
  k apply -f - >/dev/null <<YAML
apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata:
  name: chaos-partition-$1-$3
  namespace: $2
  labels:
    chaos: partition
spec:
  action: DENY
  rules:
    - from:
        - source:
            namespaces: ["$1"]
      to:
        - operation:
            ports: ["$3"]
YAML
}

chaos_heal() {
  log "chaos: heal (se eliminan las particiones)"
  k delete authorizationpolicy -A -l chaos=partition --ignore-not-found >/dev/null
}

wait_ready() {
  [ "$#" -ge 2 ] || die "uso: wait_ready <ns> <deploy> [timeout_s]"
  wr_limit=${3:-120}
  wr_sel=$(_chaos_selector "$1" "$2") || die "deployment $1/$2 no existe"
  wr_elapsed=0
  while :; do
    wr_want=$(k -n "$1" get deploy "$2" -o jsonpath='{.spec.replicas}')
    wr_have=$(_chaos_ready_pods "$1" "$wr_sel")
    wr_total=$(k -n "$1" get pods -l "$wr_sel" -o name | wc -l | tr -d ' ')
    # listo = los pods Ready coinciden con los deseados y no queda ninguno de más (ni terminando)
    if [ "$wr_have" -eq "${wr_want:-0}" ] && [ "$wr_total" -eq "${wr_want:-0}" ]; then
      return 0
    fi
    [ "$wr_elapsed" -lt "$wr_limit" ] || {
      log "wait_ready: $1/$2 no llegó a $wr_want pods Ready en ${wr_limit}s (ready=$wr_have total=$wr_total)"
      return 1
    }
    sleep 1
    wr_elapsed=$((wr_elapsed + 1))
  done
}
