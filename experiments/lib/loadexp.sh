#!/bin/sh
# Helpers de E1/E2 (consumidor caído, KEDA). Se usa con `.` desde experiments/<id>/run.sh; no se ejecuta.
# Requiere lib/common.sh, lib/expctl.sh, lib/chaos.sh y lib/metersim.sh cargados (los deja listos el runner salvo metersim).
# Port-forwards propios en 19800-19849.

LX_PIDS=""
LX_PF_PROM=19800
LX_PF_EXPORTER=19801
LX_PF_DEVICE=19803
LX_PF_BILLING=19804

# epoch de una marca RFC3339 UTC (YYYY-MM-DDTHH:MM:SS[.frac]Z); se concatena a programas awk
# shellcheck disable=SC2034,SC2016  # lo usan los scripts que cargan este archivo; es código awk
LX_EPOCH_AWK='function epoch(ts,   y, m, d, era, yoe, doy, doe) {
  y = substr(ts, 1, 4) + 0; m = substr(ts, 6, 2) + 0; d = substr(ts, 9, 2) + 0
  if (m <= 2) y -= 1
  era = int(y / 400); yoe = y - era * 400
  doy = int((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5) + d - 1
  doe = yoe * 365 + int(yoe / 4) - int(yoe / 100) + doy
  return (era * 146097 + doe - 719468) * 86400 + substr(ts, 12, 2) * 3600 + substr(ts, 15, 2) * 60 + substr(ts, 18, 2)
}
'

lx_ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
lx_now() { date -u +%s; }

# lx_bg <comando...>: lo lanza en segundo plano y guarda el PID para lx_stop
lx_bg() {
  "$@" &
  LX_PIDS="$LX_PIDS $!"
}

lx_stop() {
  # el wait dentro del grupo con 2>/dev/null evita que el shell imprima "Terminated" por cada proceso matado
  for _p in $LX_PIDS; do { kill "$_p"; wait "$_p"; } 2>/dev/null || true; done
  LX_PIDS=""
}

# lx_pf <puerto-local> <ns> <svc/x|pod/x> <puerto-remoto>: port-forward en segundo plano; espera a que abra
lx_pf() {
  kubectl --context "$KUBE_CONTEXT" -n "$2" port-forward "$3" "$1:$4" >"$OUT/logs/pf-$1.log" 2>&1 &
  LX_PIDS="$LX_PIDS $!"
  _i=0
  while ! nc -z 127.0.0.1 "$1" 2>/dev/null; do
    _i=$((_i + 1))
    [ "$_i" -lt 30 ] || { log "port-forward $1 -> $2/$3:$4 no abrió"; return 1; }
    sleep 1
  done
}

# lx_open_forwards: Prometheus, kafka-exporter, device-sim y billing-sim
lx_open_forwards() {
  lx_pf "$LX_PF_PROM" istio-system svc/prometheus 9090 || return 1
  lx_pf "$LX_PF_EXPORTER" voltera-infra svc/kafka-exporter 9308 || return 1
  lx_pf "$LX_PF_DEVICE" voltera-r1 svc/device-sim 8080 || return 1
  lx_pf "$LX_PF_BILLING" voltera-global svc/billing-sim 8080 || return 1
}

# lx_depth <vhost> <cola>: mensajes (ready + unacked) de la cola, exactos (rabbitmqctl consulta la cola; el management API
# solo refresca sus estadísticas cada 5 s)
lx_depth() {
  k -n voltera-infra exec rabbitmq-0 -c rabbitmq -- rabbitmqctl -q list_queues -p "$1" name messages \
    | awk -v q="$2" '$1 == q { print $2 }'
}

# lx_lag <grupo> <topic>: lag total del grupo, leído del exporter en cada petición (sin esperar el scrape de 15 s)
lx_lag() {
  curl -fsS -m 5 "http://127.0.0.1:$LX_PF_EXPORTER/metrics" \
    | awk -v g="$1" -v t="$2" '$0 ~ ("^kafka_consumergroup_lag_sum\\{consumergroup=\"" g "\",topic=\"" t "\"\\}") { s += $2; f = 1 } END { if (f) printf "%d\n", s; else print 0 }'
}

# lx_replicas <ns> <deploy>: "spec,ready" del deployment
lx_replicas() {
  k -n "$1" get deploy "$2" -o jsonpath='{.spec.replicas},{.status.readyReplicas}' | awk -F, '{ printf "%d,%d\n", $1, $2 }'
}

# lx_promv <promql> [tiempo]: primer valor de la consulta instantánea (vacío si no hay serie)
lx_promv() {
  if [ -n "${2:-}" ]; then
    expctl prom query "$1" --time "$2" | jq -r '.data.result[0].value[1] // empty'
  else
    expctl prom query "$1" | jq -r '.data.result[0].value[1] // empty'
  fi
}

# lx_sql <ns/statefulset> <consulta>: valor escalar (sin cabecera ni comillas)
lx_sql() { expctl sql "$1" "$2" --no-header | head -n 1 | tr -d '"'; }

# lx_sample <out.csv> <segundos> <cabecera sin ts> <función de fila>
# Una fila "ts,<columnas>" por iteración (~1-2 s) hasta agotar el tiempo. La función imprime las columnas separadas por comas.
lx_sample() {
  printf 'ts,%s\n' "$3" >"$1"
  _end=$(($(lx_now) + $2))
  while [ "$(lx_now)" -lt "$_end" ]; do
    _t=$(lx_ts)
    _row=$("$4" 2>/dev/null) || _row=
    printf '%s,%s\n' "$_t" "$_row" >>"$1"
    sleep 1
  done
}

# lx_wait <segundos> <comando...>: reintenta cada 2 s hasta que el comando pase
lx_wait() {
  _limit=$1
  shift
  _e=0
  until "$@"; do
    [ "$_e" -lt "$_limit" ] || return 1
    sleep 2
    _e=$((_e + 2))
  done
}

# lx_state <puerto> <archivo> [detail]: guarda el /state de device-sim o billing-sim (con tercer argumento, el detalle por matchId)
lx_state() {
  curl -fsS -m 20 "http://127.0.0.1:$1/state${3:+?detail=1}" >"$2"
}
