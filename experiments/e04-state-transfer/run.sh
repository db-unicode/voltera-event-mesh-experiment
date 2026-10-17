#!/bin/sh
# E4: transferencia de estado: market-projector nunca toca positions-db. Se carga con `.` desde experiments/run.sh.
# Orden: (2) ausencia de conexiones con 5 min de carga base -> (1) política -> (3) estática -> (4) autosuficiencia
# con positions-db apagada -> grafo de Kiali. (2) va antes que (1) para que el intento rechazado de (1) no entre en
# la ventana de conexiones. Región r1.
# shellcheck disable=SC2154  # OUT, EXPCTL, ROOT, HERE los define el runner

# shellcheck source=experiments/lib/probejob.sh
. "$HERE/lib/probejob.sh"
# shellcheck source=experiments/lib/redisview.sh
. "$HERE/lib/redisview.sh"

E4_NS=voltera-r1
E4_REGION=r1
E4_LOAD_S=300
E4_ATTEMPTS=20
E4_DIRECT_N=500
E4_KIALI_PORT=19851
E4_PF_PID=
E4_JOBS=
E4_DB_DOWN=0
E4_CLEANED=0

e4_cleanup() {
  [ "$E4_CLEANED" -eq 0 ] || return 0
  E4_CLEANED=1
  rv_watch_stop
  [ -z "$E4_PF_PID" ] || kill "$E4_PF_PID" 2>/dev/null || true
  for j in $E4_JOBS; do k -n "$E4_NS" delete job "$j" --ignore-not-found --wait=false >/dev/null 2>&1 || true; done
  if [ "$E4_DB_DOWN" -eq 1 ]; then
    log "restaura positions-db a 1 réplica"
    k -n "$E4_NS" scale statefulset positions-db --replicas=1 >/dev/null || true
    k -n "$E4_NS" rollout status statefulset/positions-db --timeout=180s >/dev/null || true
  fi
}

e4_db_sql() { expctl sql "$E4_NS/positions-db" "$@"; }

# e4_rbac <denied|allowed>: contador Envoy tcp.rbac.<x> del sidecar de positions-db
e4_rbac() {
  k -n "$E4_NS" exec positions-db-0 -c istio-proxy -- pilot-agent request GET stats 2>/dev/null \
    | awk -F': ' -v key="tcp.rbac.$1" '$1 == key { print $2; found = 1 } END { if (!found) print 0 }'
}

# e4_attempt_job <nombre> <serviceAccount>: Job con N intentos de psql contra positions-db:5432
e4_attempt_job() {
  k -n "$E4_NS" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $1
  labels: {app: e04-attempt, experiment: e03-e04-e07}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 300
  ttlSecondsAfterFinished: 900
  template:
    metadata:
      labels: {app: e04-attempt, experiment: e03-e04-e07}
    spec:
      serviceAccountName: $2
      restartPolicy: Never
      containers:
        - name: psql
          image: postgres:18.6
          imagePullPolicy: IfNotPresent
          env:
            - name: PGPASSWORD
              valueFrom: {secretKeyRef: {name: positions-db-credentials, key: POSTGRES_PASSWORD}}
          command:
            - sh
            - -c
            - |
              i=1
              while [ \$i -le $E4_ATTEMPTS ]; do
                out=\$(psql 'host=positions-db port=5432 user=voltera dbname=voltera sslmode=disable connect_timeout=5' -Atc "select 'connected'" 2>&1)
                rc=\$?
                echo "attempt=\$i sa=$2 rc=\$rc out=\$(echo "\$out" | head -n 1)"
                i=\$((i + 1))
                sleep 0.5
              done
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {memory: 64Mi}
YAML
}

e4_wait_job() { # <nombre> -> imprime logs; 0 si terminó
  _i=0
  while [ "$_i" -lt 180 ]; do
    _st=$(k -n "$E4_NS" get job "$1" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $_st in 1/* | */1) break ;; esac
    _i=$((_i + 1))
    sleep 1
  done
  k -n "$E4_NS" logs "job/$1" -c psql 2>&1
}

exp_reset() {
  trap 'e4_cleanup; cluster_unlock; exit 130' INT TERM
  trap 'e4_cleanup; cluster_unlock' EXIT
  need go
  need jq
  need curl
  probejob_build || return 1
  # la política y el application_name del gateway son parte del despliegue definitivo; aquí solo se garantizan
  MISE_PROJECT_ROOT=$ROOT sh "$ROOT/mise-tasks/security/up" "$E4_REGION" || return 1
  url=$(k -n "$E4_NS" get deploy declaration-gateway -o json \
    | jq -r '[.spec.template.spec.containers[0].env[] | select(.name == "DATABASE_URL") | .value][0] // ""')
  case $url in
    *application_name=declaration-gateway*) ;;
    *)
      log "el gateway no fija application_name: se aplica deploy/apps/$E4_REGION/write-side"
      k apply -k "$ROOT/deploy/apps/$E4_REGION/write-side" >/dev/null || return 1
      ;;
  esac
  k -n "$E4_NS" scale statefulset positions-db --replicas=1 >/dev/null || return 1
  k -n "$E4_NS" rollout status statefulset/positions-db --timeout=180s >/dev/null || return 1
  probejob_direct_secret "$E4_REGION" || return 1
  k -n "$E4_NS" delete job -l app=e04-attempt --ignore-not-found >/dev/null || true
}

drive() {
  RUN=$(date -u +%y%m%d%H%M%S)
  printf '%s\n' "$RUN" >"$OUT/raw/run.txt"

  # ---- (2) ausencia de conexiones: 5 min de carga base (3000 medidores / 30 s = 100 decl/s) ----
  log "(2) carga base ${E4_LOAD_S}s y muestreo de pg_stat_activity cada 15 s"
  t2_start=$(expctl now)
  probejob_start "$E4_REGION" amqp --profile steady --prefix "e04-$RUN-s-" --meters 3000 --period 30 \
    --duration "$E4_LOAD_S" --seed 4 || return 1
  load_job=$PROBEJOB_LAST
  E4_JOBS="$load_job"
  q="select usename, application_name, coalesce(client_addr::text, '') as client_addr, count(*) as n from pg_stat_activity where datname = 'voltera' and backend_type = 'client backend' and pid <> pg_backend_pid() group by 1, 2, 3 order by 1, 2, 3"
  pg_csv=$OUT/pg_stat_activity.csv
  printf 'ts,usename,application_name,client_addr,n\n' >"$pg_csv"
  _end=$(($(date -u +%s) + E4_LOAD_S + 40))
  while [ "$(date -u +%s)" -lt "$_end" ]; do
    st=$(k -n "$E4_NS" get job "$load_job" -o jsonpath='{.status.succeeded}/{.status.failed}')
    ts=$(expctl now)
    e4_db_sql "$q" --no-header | awk -v ts="$ts" 'NF { print ts "," $0 }' >>"$pg_csv" || true
    case $st in 1/* | */1) break ;; esac
    sleep 15
  done
  probejob_wait "$E4_REGION" "$load_job" 60 >"$OUT/logs/steady.log" || log "la carga steady terminó con error (ver logs/steady.log)"
  sleep 20 # un scrape de Prometheus (15 s)
  t2_end=$(expctl now)
  w=$(expctl elapsed "$t2_start" "$t2_end" | awk '{ printf "%d", $1 + 0.5 }')
  # conexiones nuevas por origen en la ventana = contador ahora - contador hace w s (origen nuevo: desde 0). `increase`
  # no sirve: una serie que nace dentro de la ventana con una sola muestra devuelve 0
  m="istio_tcp_connections_opened_total{destination_workload=\"positions-db\",destination_workload_namespace=\"$E4_NS\",reporter=\"destination\"}"
  expctl prom query "sum by (source_workload)($m) - (sum by (source_workload)($m offset ${w}s) or sum by (source_workload)($m) * 0)" \
    --time "$t2_end" >"$OUT/istio_tcp.json" || return 1

  # ---- (1) política: 20 intentos desde un pod con SA market-projector (y control con SA declaration-gateway) ----
  log "(1) $E4_ATTEMPTS intentos con SA market-projector y con SA declaration-gateway (control)"
  denied0=$(e4_rbac denied)
  allowed0=$(e4_rbac allowed)
  e4_attempt_job "e04-deny-$RUN" market-projector || return 1
  e4_attempt_job "e04-control-$RUN" declaration-gateway || return 1
  E4_JOBS="$E4_JOBS e04-deny-$RUN e04-control-$RUN"
  e4_wait_job "e04-deny-$RUN" >"$OUT/deny.log" || true
  e4_wait_job "e04-control-$RUN" >"$OUT/control.log" || true
  denied1=$(e4_rbac denied)
  allowed1=$(e4_rbac allowed)
  printf '{"denied_before":%s,"denied_after":%s,"allowed_before":%s,"allowed_after":%s}\n' \
    "$denied0" "$denied1" "$allowed0" "$allowed1" >"$OUT/rbac_stats.json"
  sleep 20
  # contadores absolutos: el pod de prueba rechazado figura como source_workload (e04-deny-*), nunca como market-projector
  expctl prom query "sum by (source_workload)($m)" >"$OUT/istio_tcp_after_deny.json" || true

  # ---- (3) estática ----
  log "(3) go list y variables del Deployment"
  (cd "$ROOT" && go list -deps ./cmd/market-projector) >"$OUT/go_deps.txt" || return 1
  (cd "$ROOT" && go list -deps ./cmd/declaration-gateway) >"$OUT/go_deps_gateway.txt" || return 1
  (cd "$ROOT" && go list -deps -f '{{.ImportPath}} {{join .Imports " "}}' ./cmd/market-projector) \
    | awk '{ for (i = 2; i <= NF; i++) if ($i == "database/sql") { print $1; break } }' >"$OUT/go_sql_importers.txt" || true
  k -n "$E4_NS" get deploy market-projector -o json >"$OUT/projector_deployment.json" || return 1

  # ---- (4) autosuficiencia con positions-db apagada ----
  log "(4) positions-db a 0 réplicas y $E4_DIRECT_N PositionDeclared directos a energy-positions"
  E4_DB_DOWN=1
  k -n "$E4_NS" scale statefulset positions-db --replicas=0 >/dev/null || return 1
  k -n "$E4_NS" wait --for=delete pod/positions-db-0 --timeout=120s >/dev/null || return 1
  glob="pos:*:e04-$RUN-d-*"
  rv_watch_start "$glob" "$OUT/raw/redis_direct.txt" 0
  sleep 3
  probejob_run "$E4_REGION" direct --profile direct-positions --prefix "e04-$RUN-d-" --meters "$E4_DIRECT_N" --rate 0 \
    --seed 4 --zones 4 >"$OUT/logs/direct.log" || log "el publicador directo terminó con error (ver logs/direct.log)"
  _i=0
  while [ "$_i" -lt 30 ]; do
    [ "$(tail -n 1 "$OUT/raw/redis_direct.txt" | awk -F';' '{ split($2, a, " "); print a[2] + 0 }')" -ge "$E4_DIRECT_N" ] && break
    _i=$((_i + 1))
    sleep 1
  done
  rv_watch_stop
  k -n "$E4_NS" scale statefulset positions-db --replicas=1 >/dev/null || return 1
  k -n "$E4_NS" rollout status statefulset/positions-db --timeout=180s >/dev/null || return 1
  E4_DB_DOWN=0

  # ---- grafo de Kiali (aristas TCP) ----
  log "grafo de Kiali"
  kubectl --context "$KUBE_CONTEXT" -n istio-system port-forward svc/kiali "$E4_KIALI_PORT:20001" >/dev/null 2>&1 &
  E4_PF_PID=$!
  _i=0
  until curl -sf "http://127.0.0.1:$E4_KIALI_PORT/kiali/api/status" >/dev/null 2>&1; do
    _i=$((_i + 1))
    [ "$_i" -lt 20 ] || break
    sleep 1
  done
  dur=$(expctl elapsed "$t2_start" "$(expctl now)" | awk '{ printf "%d", $1 + 60 }')
  curl -s "http://127.0.0.1:$E4_KIALI_PORT/kiali/api/namespaces/graph?namespaces=$E4_NS&graphType=workload&duration=${dur}s&includeIdleEdges=false&injectServiceNodes=false&appenders=deadNode,istio,responseTime,throughput" \
    >"$OUT/kiali_graph.json" || log "sin grafo de Kiali"
  return 0
}

collect() {
  t0=$1
  # (1)
  rejected=$(grep -c ' rc=[1-9]' "$OUT/deny.log" || true)
  connected=$(grep -c ' rc=0 ' "$OUT/deny.log" || true)
  auth_err=$(grep -ci 'authentication' "$OUT/deny.log" || true)
  attempts=$(grep -c '^attempt=' "$OUT/deny.log" || true)
  c_connected=$(grep -c ' rc=0 ' "$OUT/control.log" || true)
  c_attempts=$(grep -c '^attempt=' "$OUT/control.log" || true)

  # (2)
  foreign_rows=$(awk -F, 'NR > 1 && $3 != "declaration-gateway" { n++ } END { print n + 0 }' "$OUT/pg_stat_activity.csv")
  pg_rows=$(awk -F, 'NR > 1 { n++ } END { print n + 0 }' "$OUT/pg_stat_activity.csv")
  pg_clients=$(awk -F, 'NR > 1 { print $2 "/" $3 }' "$OUT/pg_stat_activity.csv" | sort -u | jq -R . | jq -sc .)
  prom_sources=$(jq -c '[.data.result[] | select((.value[1] | tonumber) > 0) | .metric.source_workload]' "$OUT/istio_tcp.json")
  prom_foreign=$(jq '[.data.result[] | select((.value[1] | tonumber) > 0) | select(.metric.source_workload != "declaration-gateway")] | length' "$OUT/istio_tcp.json")
  prom_projector=$(jq '[.data.result[] | select(.metric.source_workload == "market-projector") | .value[1] | tonumber] | add // 0' "$OUT/istio_tcp.json")
  prom_gateway=$(jq '[.data.result[] | select(.metric.source_workload == "declaration-gateway") | .value[1] | tonumber] | add // 0' "$OUT/istio_tcp.json")
  prom_after_deny=$(jq -c '[.data.result[] | select((.value[1] | tonumber) > 0) | {(.metric.source_workload): (.value[1] | tonumber | floor)}] | add // {}' "$OUT/istio_tcp_after_deny.json" 2>/dev/null || echo '{}')

  # (3)
  # drivers de Postgres (pgx, jackc, lib/pq). database/sql de la stdlib NO es un driver: entra por prometheus/collectors
  # (DBStatsCollector) a través de internal/telemetry y se informa aparte (static.stdlib_database_sql_importers)
  db_deps=$(grep -Ec 'pgx|jackc|lib/pq' "$OUT/go_deps.txt" || true)
  gw_db_deps=$(grep -Ec 'pgx|jackc|lib/pq' "$OUT/go_deps_gateway.txt" || true)
  sql_importers=$(cat "$OUT/go_sql_importers.txt" 2>/dev/null | jq -R . | jq -sc . || echo '[]')
  deps_total=$(wc -l <"$OUT/go_deps.txt" | tr -d ' ')
  env_matches=$(jq '[.spec.template.spec.containers[] | ((.env // [])[] | [.name, (.value // ""), (.valueFrom.secretKeyRef.name // "")] | join(" ")), ((.envFrom // [])[] | (.secretRef.name // .configMapRef.name // ""))] | map(select(test("positions|postgres|DATABASE_URL|PG_|DSN"; "i"))) | length' "$OUT/projector_deployment.json")
  jq '[.spec.template.spec.containers[] | (.env // [])[] | .name]' "$OUT/projector_deployment.json" >"$OUT/projector_env_names.json"

  # (4)
  published=$(grep '^{"profile"' "$OUT/logs/direct.log" | tail -n 1 | jq '.published // 0' 2>/dev/null || echo 0)
  pub_start=$(grep '^{"profile"' "$OUT/logs/direct.log" | tail -n 1 | jq -r '.startedAt // empty' 2>/dev/null || true)
  in_redis=$(tail -n 1 "$OUT/raw/redis_direct.txt" | awk -F';' '{ split($2, a, " "); print a[2] + 0 }')
  full_ts=$(awk -F';' -v n="$E4_DIRECT_N" '{ split($2, a, " "); if (a[2] + 0 >= n) { print $1; exit } }' "$OUT/raw/redis_direct.txt")
  if [ -n "$full_ts" ] && [ -n "$pub_start" ]; then
    to_full=$(expctl elapsed "$pub_start" "$full_ts")
  else
    to_full=-1
  fi
  rv_watch_csv "$OUT/raw/redis_direct.txt" "$OUT/raw/redis_direct.csv"

  # grafo de Kiali: aristas entre workloads (id de nodo -> workload)
  kiali_pg=$(jq '[.elements.nodes[]?.data | select(.workload == "positions-db") | .id] | length' "$OUT/kiali_graph.json" 2>/dev/null || echo 0)
  kiali_proj=$(jq '(.elements.nodes // []) as $n | ([$n[].data | select(.workload == "market-projector") | .id]) as $p | ([$n[].data | select(.workload == "positions-db") | .id]) as $d | [(.elements.edges // [])[].data | select((.source as $s | $p | index($s)) and (.target as $t | $d | index($t)))] | length' "$OUT/kiali_graph.json" 2>/dev/null || echo 0)
  kiali_gw=$(jq '(.elements.nodes // []) as $n | ([$n[].data | select(.workload == "declaration-gateway") | .id]) as $p | ([$n[].data | select(.workload == "positions-db") | .id]) as $d | [(.elements.edges // [])[].data | select((.source as $s | $p | index($s)) and (.target as $t | $d | index($t)))] | length' "$OUT/kiali_graph.json" 2>/dev/null || echo 0)

  jq -n \
    --argjson attempts "${attempts:-0}" --argjson rejected "${rejected:-0}" --argjson connected "${connected:-0}" \
    --argjson auth_err "${auth_err:-0}" --argjson c_attempts "${c_attempts:-0}" --argjson c_connected "${c_connected:-0}" \
    --slurpfile rbac "$OUT/rbac_stats.json" \
    --argjson pg_rows "$pg_rows" --argjson foreign_rows "$foreign_rows" --argjson pg_clients "$pg_clients" \
    --argjson prom_sources "$prom_sources" --argjson prom_foreign "$prom_foreign" --argjson prom_projector "$prom_projector" \
    --argjson prom_gateway "$prom_gateway" --argjson prom_after_deny "$prom_after_deny" \
    --argjson db_deps "$db_deps" --argjson gw_db_deps "$gw_db_deps" --argjson deps_total "$deps_total" --argjson env_matches "$env_matches" --argjson sql_importers "$sql_importers" \
    --argjson published "${published:-0}" --argjson in_redis "${in_redis:-0}" --argjson to_full "$to_full" \
    --argjson kiali_pg "$kiali_pg" --argjson kiali_proj "$kiali_proj" --argjson kiali_gw "$kiali_gw" \
    '{policy: {attempts: $attempts, rejected: $rejected, connected: $connected, auth_errors: $auth_err,
               control_attempts: $c_attempts, control_connected: $c_connected,
               rbac_denied_delta: ($rbac[0].denied_after - $rbac[0].denied_before),
               rbac_allowed_delta: ($rbac[0].allowed_after - $rbac[0].allowed_before)},
      connections: {pg_stat_samples: $pg_rows, pg_foreign_rows: $foreign_rows, pg_clients: $pg_clients,
                    prom_sources: $prom_sources, prom_foreign_sources: $prom_foreign,
                    prom_projector_connections: $prom_projector, prom_gateway_connections: $prom_gateway,
                    prom_sources_after_deny_test: $prom_after_deny},
      static: {projector_db_deps: $db_deps, gateway_db_deps_control: $gw_db_deps, projector_deps_total: $deps_total,
               projector_env_matches: $env_matches,
               stdlib_database_sql_importers: $sql_importers},
      autonomy: {published: $published, in_redis: $in_redis, seconds_to_full: $to_full},
      kiali: {positions_db_nodes: $kiali_pg, edges_projector_to_db: $kiali_proj, edges_gateway_to_db: $kiali_gw}}' >"$OUT/metrics.json"
  : "$t0"
}
