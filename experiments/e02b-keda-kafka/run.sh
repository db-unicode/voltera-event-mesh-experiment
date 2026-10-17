#!/bin/sh
# E2 parte B: KEDA sobre el lag de Kafka (energy-transfer 0 -> N -> 0 con 3000 EnergyMatched en ~10 s).
# Publica directo al topic con `kafka-probe produce-matched` (EnergyMatched válidos con prefijo), con la identidad
# (ServiceAccount) de matching-engine, que es el publicador legítimo del topic.
# Se carga con `.` desde experiments/run.sh (define drive y collect).
# shellcheck shell=sh disable=SC2034,SC2329  # las funciones las invoca el runner
# shellcheck source=experiments/lib/loadexp.sh
. "$HERE/lib/loadexp.sh"

E2B_NS=voltera-r1
E2B_TOPIC=r1.energy-matched
E2B_COUNT=${E2B_COUNT:-3000}
E2B_RATE=${E2B_RATE:-300}                  # 3000 eventos en ~10 s
E2B_DRAIN_LIMIT_S=${E2B_DRAIN_LIMIT_S:-300}
E2B_SCALE_IN_LIMIT_S=${E2B_SCALE_IN_LIMIT_S:-240}
E2B_MAX_REPLICAS=6                         # = particiones del topic

e2b_cleanup() {
  lx_stop
  chaos_resume_keda "$E2B_NS" energy-transfer >/dev/null 2>&1 || true
}
trap 'e2b_cleanup; cluster_unlock' EXIT INT TERM

E2B_COLS=lag,et,et_ready
e2b_row() {
  _lag=$(lx_lag energy-transfer "$E2B_TOPIC")
  _r=$(lx_replicas "$E2B_NS" energy-transfer)
  echo "$_lag,$_r"
}

e2b_idle() {
  [ "$(lx_lag energy-transfer "$E2B_TOPIC")" = 0 ] \
    && [ "$(lx_replicas "$E2B_NS" energy-transfer)" = "0,0" ] \
    && [ "$(lx_sql "$E2B_NS/matches-db" "select count(*) from outbox where published_at is null")" = 0 ]
}
e2b_lag_zero() { [ "$(lx_lag energy-transfer "$E2B_TOPIC")" = 0 ]; }
e2b_et_zero() { [ "$(lx_replicas "$E2B_NS" energy-transfer)" = "0,0" ]; }
# e2b_billing_complete: billing-sim tiene todos los matchId del prefijo (guarda su /state en raw/billing.json)
e2b_billing_complete() {
  lx_state "$LX_PF_BILLING" "$OUT/raw/billing.json" detail || return 1
  jq -e --slurpfile ids "$OUT/raw/ids.json" '[$ids[0][] as $i | select((.byMatchId[$i].count // 0) == 0)] | length == 0' "$OUT/raw/billing.json" >/dev/null
}
e2b_all_sent() {
  [ "$(lx_sql "$E2B_NS/transfer-db" "select count(*) from transfers where seller_meter_id like '${E2B_PREFIX}-s%' and status = 'SENT'")" = "$E2B_COUNT" ]
}

exp_reset() {
  mkdir -p "$OUT/logs"
  chaos_resume_keda "$E2B_NS" energy-transfer >/dev/null 2>&1 || true
  # la imagen debe incluir el subcomando produce-matched
  MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/images/build" kafka-probe >"$OUT/logs/build-kafka-probe.log" 2>&1 || { log "falló la imagen kafka-probe"; return 1; }
  lx_open_forwards || return 1
  log "esperando energy-transfer en 0 réplicas, lag 0 y outbox de matches vacía"
  lx_wait 300 e2b_idle || { log "energy-transfer no quedó en 0 con lag 0"; return 1; }
}

# e2b_produce: Job de kafka-probe produce-matched; devuelve 0 si terminó bien
e2b_produce() {
  _job=e02b-produce-$(lx_now)
  k -n "$E2B_NS" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $_job
  labels: {app: kafka-probe-job}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 240
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: {app: kafka-probe-job}
    spec:
      serviceAccountName: matching-engine
      restartPolicy: Never
      containers:
        - name: probe
          image: voltera/kafka-probe:dev
          imagePullPolicy: IfNotPresent
          args: ["produce-matched", "--topic", "$E2B_TOPIC", "--region", "r1", "--count", "$E2B_COUNT", "--rate", "$E2B_RATE", "--prefix", "$E2B_PREFIX"]
          env:
            - {name: KAFKA_BROKERS, value: "kafka.voltera-infra.svc:9092"}
          resources:
            requests: {cpu: 10m, memory: 32Mi}
            limits: {memory: 256Mi}
YAML
  _i=0
  _rc=
  while [ "$_i" -lt 150 ]; do
    _st=$(k -n "$E2B_NS" get job "$_job" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $_st in
      1/*) _rc=0; break ;;
      */1) _rc=1; break ;;
    esac
    _i=$((_i + 1))
    sleep 2
  done
  [ "$_rc" ] || _rc=1
  k -n "$E2B_NS" logs "job/$_job" -c probe >"$OUT/logs/produce-matched.log" 2>&1 || true
  return "$_rc"
}

drive() {
  E2B_PREFIX="e02b-$(lx_now)"
  log "prefijo $E2B_PREFIX; publicando $E2B_COUNT EnergyMatched a $E2B_RATE/s"
  _dev0=$(curl -fsS -m 20 "http://127.0.0.1:$LX_PF_DEVICE/state" | jq -c '{total, distinct, duplicates}')
  lx_sample "$OUT/raw/series.csv" 3000 "$E2B_COLS" e2b_row &
  _s1=$!
  expctl sample --interval 2s --duration 3000s --out "$OUT/raw/replicas.csv" \
    --replicas "et=$E2B_NS/energy-transfer" \
    --prom "lag_prom=kafka_consumergroup_lag_sum{consumergroup=\"energy-transfer\",topic=\"$E2B_TOPIC\"}" 2>"$OUT/logs/sample.log" &
  _s2=$!
  sleep 10
  _t_prod=$(lx_now)
  _ok=1
  e2b_produce || { log "falló produce-matched (ver logs/produce-matched.log)"; _ok=0; }
  _t_prod_end=$(lx_now)
  log "publicado; esperando lag 0 (máx ${E2B_DRAIN_LIMIT_S}s)"
  _drained=1
  lx_wait "$E2B_DRAIN_LIMIT_S" e2b_lag_zero || _drained=0
  lx_wait "$E2B_DRAIN_LIMIT_S" e2b_all_sent || log "no están las $E2B_COUNT transferencias SENT"
  _t_lag0=$(lx_now)
  log "esperando scale-to-zero (máx ${E2B_SCALE_IN_LIMIT_S}s)"
  _zeroed=1
  lx_wait "$E2B_SCALE_IN_LIMIT_S" e2b_et_zero || _zeroed=0
  _t_zero=$(lx_now)
  k -n "$E2B_NS" get scaledobject energy-transfer -o json >"$OUT/raw/keda-scaledobject.json" || true
  k -n "$E2B_NS" get hpa -o json >"$OUT/raw/keda-hpa.json" || true
  k -n "$E2B_NS" get events --sort-by=.lastTimestamp -o json >"$OUT/raw/keda-events.json" || true
  sleep 4
  kill "$_s1" "$_s2" 2>/dev/null || true
  jq -n --arg prefix "$E2B_PREFIX" --argjson dev0 "$_dev0" --argjson tProd "$_t_prod" --argjson tProdEnd "$_t_prod_end" \
    --argjson tLag0 "$_t_lag0" --argjson tZero "$_t_zero" --argjson produced "$_ok" --argjson drained "$_drained" --argjson zeroed "$_zeroed" \
    '{prefix: $prefix, device0: $dev0, tProd: $tProd, tProdEnd: $tProdEnd, tLag0Wait: $tLag0, tZeroWait: $tZero,
      produced: ($produced == 1), drained: ($drained == 1), zeroed: ($zeroed == 1)}' >"$OUT/raw/phase.json"
  [ "$_ok" = 1 ] && [ "$_drained" = 1 ] && [ "$_zeroed" = 1 ]
}

# ---- recolección ----

collect() {
  _ph="$OUT/raw/phase.json"
  _prefix=$(jq -r .prefix "$_ph")
  _tprod=$(jq -r .tProd "$_ph")
  # ids de las transferencias del prefijo, y su efecto en device-sim
  expctl sql "$E2B_NS/transfer-db" "select match_id from transfers where seller_meter_id like '${_prefix}-s%' order by 1" --no-header \
    | tr -d '"' | jq -R . | jq -s . >"$OUT/raw/ids.json" || return 1
  lx_state "$LX_PF_DEVICE" "$OUT/raw/device.json" || return 1
  # billing-sim recibe por Kafka -> mesh-bridge -> NATS -> leafnode -> stream: puede ir unos segundos por detrás
  lx_wait 120 e2b_billing_complete || log "billing-sim no recibió todos los matches en 120 s"
  _sent=$(lx_sql "$E2B_NS/transfer-db" "select count(*) from transfers where seller_meter_id like '${_prefix}-s%' and status = 'SENT'")
  _failed=$(lx_sql "$E2B_NS/transfer-db" "select count(*) from transfers where seller_meter_id like '${_prefix}-s%' and status <> 'SENT'")
  # shellcheck disable=SC2046  # la salida son enteros separados por espacios
  set -- $(awk -F, -v tprod="$_tprod" "$LX_EPOCH_AWK"'
    BEGIN { pl = 0; pr = 0; first = -1; r0 = -1; lag0 = -1; zero = -1; last = -1; lastlag = -1; peakrt = -1 }
    NR == 1 { next }
    $2 == "" || $3 == "" { next }
    { t = epoch($1); l = $2 + 0; r = $3 + 0
      if (r0 < 0) r0 = r
      if (l > pl) { pl = l; peakt = t }
      if (r > pr) pr = r
      if (t >= tprod && first < 0 && r > 0) first = t
      if (pl >= 1000 && lag0 < 0 && l == 0 && t >= peakt) lag0 = t
      if (lag0 >= 0 && zero < 0 && r == 0 && t >= lag0) zero = t
      last = r; lastlag = l }
    END { printf "%d %d %d %d %d %d %d %d\n", pl, pr, r0, first, lag0, zero, last, lastlag }' "$OUT/raw/series.csv")
  _pl=$1 _pr=$2 _r0=$3 _first=$4 _lag0=$5 _zero=$6 _lastr=$7 _lastlag=$8
  _t1=$(jq -r .tProdEnd "$_ph")
  _firstS=-1 _lag0S=-1 _zeroS=-1
  [ "$_first" -ge 0 ] && _firstS=$((_first - _tprod))
  [ "$_lag0" -ge 0 ] && _lag0S=$((_lag0 - _tprod))
  [ "$_lag0" -ge 0 ] && [ "$_zero" -ge 0 ] && _zeroS=$((_zero - _lag0))
  jq -n --slurpfile ph "$_ph" --slurpfile ids "$OUT/raw/ids.json" --slurpfile dev "$OUT/raw/device.json" --slurpfile bil "$OUT/raw/billing.json" \
    --argjson count "$E2B_COUNT" --argjson sent "$_sent" --argjson failed "$_failed" --argjson pl "$_pl" --argjson pr "$_pr" \
    --argjson r0 "$_r0" --argjson firstS "$_firstS" --argjson lag0S "$_lag0S" --argjson zeroS "$_zeroS" \
    --argjson lastR "$_lastr" --argjson lastLag "$_lastlag" --argjson max "$E2B_MAX_REPLICAS" '
    $ids[0] as $I
    | {prefix: $ph[0].prefix, produced: $ph[0].produced, drained: $ph[0].drained, zeroed: $ph[0].zeroed,
       published: $count, transfersTotal: ($I | length), transfersSent: $sent, transfersNotSent: $failed,
       device: {missing: [$I[] as $i | select(($dev[0].byMatchId[$i] // 0) == 0)] | length,
                dups:    [$I[] as $i | select(($dev[0].byMatchId[$i] // 0) > 1)] | length,
                deltaDistinct: ($dev[0].distinct - $ph[0].device0.distinct)},
       billing: {missing: [$I[] as $i | select(($bil[0].byMatchId[$i].count // 0) == 0)] | length},
       replicasStart: $r0, peakReplicas: $pr, maxReplicas: $max, replicasEnd: $lastR,
       peakLag: $pl, lagEnd: $lastLag,
       timeToFirstReplicaS: $firstS, timeToLagZeroS: $lag0S, scaleToZeroS: $zeroS}' >"$OUT/metrics.json" || return 1
  # figura: lag y réplicas x500 en el mismo eje
  awk -F, 'NR == 1 { print "ts,lag,replicas_x500"; next } $2 == "" || $3 == "" { next } { print $1 "," $2 "," $3 * 500 }' "$OUT/raw/series.csv" >"$OUT/figures/lag_replicas.csv"
  expctl plot --csv "$OUT/figures/lag_replicas.csv" --x ts --title "E2B: lag de energy-transfer y réplicas x500" \
    --out "$OUT/figures/e02b-lag-replicas.png" || log "sin figura lag/réplicas"
  return 0
}
