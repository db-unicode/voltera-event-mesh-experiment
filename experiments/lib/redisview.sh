#!/bin/sh
# Acceso a la vista Redis `market-view` desde el host (kubectl exec + redis-cli dentro del pod; usa REDISCLI_AUTH del
# contenedor) para E3, E4 y E7. Se usa con `.`; no se ejecuta. Requiere common.sh cargado antes.
#
# RV_NS (voltera-r1) es el namespace de la región. Los patrones son globs de Redis sobre la key completa
# (p. ej. 'pos:*:e07-123-*'). KEYS es O(N) en el total de keys: aceptable aquí (miles de keys, 1 Hz como mucho).
#
#   rv_cli <args...>                  redis-cli dentro del pod
#   rv_count <glob>                   nº de keys
#   rv_now_us                         reloj del servidor Redis en µs desde epoch (mismo reloj que el host)
#   rv_delete <glob>                  borra las keys del patrón (nunca FLUSHALL) e imprime cuántas
#   rv_state <glob>                   una línea por key: meterId|side|kwh|validUntil|_v
#   rv_watch_start <glob> <archivo> [detalle=1]   muestreo 1 Hz en segundo plano; PID en RV_WATCH_PID
#   rv_watch_stop
#   rv_watch_csv <archivo> <csv>      ts,keys,min_ttl_s (min_ttl_s vacío si no hay keys)
#
# Formato del archivo de rv_watch_start, una línea por segundo:
#   <ts RFC3339 ns del contenedor>;<t_us servidor> <nº keys> <min PTTL ms o -1> [<key>,<PTTL ms>,<projectedAt µs>]...

RV_NS=${RV_NS:-voltera-r1}
RV_WATCH_PID=

RV_LUA_WATCH=$(
  cat <<'EOF'
local function days_from_civil(y, m, d)
  if m <= 2 then y = y - 1 end
  local era = math.floor(y / 400)
  local yoe = y - era * 400
  local mp = (m + 9) % 12
  local doy = math.floor((153 * mp + 2) / 5) + d - 1
  local doe = yoe * 365 + math.floor(yoe / 4) - math.floor(yoe / 100) + doy
  return era * 146097 + doe - 719468
end
local function parse(s)
  if not s then return 0 end
  local Y, M, D, h, mi, sec, frac = string.match(s, '^(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)%.?(%d*)Z$')
  if not Y then return 0 end
  local us = 0
  if frac ~= '' then us = tonumber(string.sub(frac .. '000000', 1, 6)) end
  return ((days_from_civil(tonumber(Y), tonumber(M), tonumber(D)) * 24 + tonumber(h)) * 60 + tonumber(mi)) * 60 * 1000000
    + tonumber(sec) * 1000000 + us
end
local t = redis.call('TIME')
local keys = redis.call('KEYS', ARGV[1])
local detail = ARGV[2] ~= '0'
local parts = {}
local n, minttl = 0, -1
for _, k in ipairs(keys) do
  local ttl = redis.call('PTTL', k)
  if ttl >= 0 then
    n = n + 1
    if minttl < 0 or ttl < minttl then minttl = ttl end
    if detail then
      parts[#parts + 1] = k .. ',' .. ttl .. ',' .. string.format('%d', parse(redis.call('HGET', k, 'projectedAt')))
    end
  end
end
local head = string.format('%d', t[1] * 1000000 + t[2]) .. ' ' .. n .. ' ' .. minttl
if #parts > 0 then head = head .. ' ' .. table.concat(parts, ' ') end
return head
EOF
)

RV_LUA_STATE=$(
  cat <<'EOF'
local out = {}
for _, k in ipairs(redis.call('KEYS', ARGV[1])) do
  local h = redis.call('HMGET', k, 'meterId', 'side', 'kwh', 'validUntil', '_v')
  if h[1] and h[5] then
    out[#out + 1] = h[1] .. '|' .. tostring(h[2]) .. '|' .. tostring(h[3]) .. '|' .. tostring(h[4]) .. '|' .. h[5]
  end
end
return out
EOF
)

RV_LUA_DEL=$(
  cat <<'EOF'
local n = 0
for _, k in ipairs(redis.call('KEYS', ARGV[1])) do
  n = n + redis.call('DEL', k)
end
return n
EOF
)

rv_cli() { k -n "$RV_NS" exec deploy/market-view -c redis -- redis-cli --no-auth-warning "$@"; }

rv_count() { rv_cli EVAL "return #redis.call('KEYS', ARGV[1])" 0 "$1"; }

rv_now_us() { rv_cli EVAL "local t = redis.call('TIME') return string.format('%d', t[1] * 1000000 + t[2])" 0; }

rv_delete() { rv_cli EVAL "$RV_LUA_DEL" 0 "$1"; }

rv_state() { rv_cli EVAL "$RV_LUA_STATE" 0 "$1"; }

RV_WATCH_SEQ=0
RV_WATCH_TAG=

# El bucle remoto no muere cuando se cierra el exec (el runtime mantiene la tubería): termina con un archivo de parada
# (rv_watch_stop) o a los RV_WATCH_MAX_S (1800) segundos.
rv_watch_start() {
  RV_WATCH_SEQ=$((RV_WATCH_SEQ + 1))
  RV_WATCH_TAG=$$-$RV_WATCH_SEQ
  # kubectl directo (no la función k): $! debe ser el PID de kubectl para poder matarlo. El script remoto va entre comillas
  # simples a propósito: lo expande el sh del contenedor, no el nuestro.
  # shellcheck disable=SC2016
  kubectl --context "$KUBE_CONTEXT" -n "$RV_NS" exec deploy/market-view -c redis -- env "LUA=$RV_LUA_WATCH" "PAT=$1" \
    "DETAIL=${3:-1}" "TAG=$RV_WATCH_TAG" "MAXS=${RV_WATCH_MAX_S:-1800}" sh -c \
    'end=$(( $(date +%s) + MAXS )); while [ ! -f "/tmp/rvstop.$TAG" ] && [ "$(date +%s)" -lt "$end" ]; do printf "%s;" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"; redis-cli --no-auth-warning EVAL "$LUA" 0 "$PAT" "$DETAIL"; sleep 1; done; rm -f "/tmp/rvstop.$TAG"' \
    >"$2" 2>/dev/null &
  RV_WATCH_PID=$!
}

rv_watch_stop() {
  if [ -n "$RV_WATCH_PID" ]; then
    kubectl --context "$KUBE_CONTEXT" -n "$RV_NS" exec deploy/market-view -c redis -- touch "/tmp/rvstop.$RV_WATCH_TAG" >/dev/null 2>&1 || true
    { kill "$RV_WATCH_PID"; wait "$RV_WATCH_PID"; } 2>/dev/null || true
  fi
  RV_WATCH_PID=
}

rv_watch_csv() {
  awk -F';' 'BEGIN { print "ts,keys,min_ttl_s" }
    NF >= 2 { n = split($2, a, " "); if (n < 3) next
              printf "%s,%d,%s\n", $1, a[2], (a[3] < 0 ? "" : a[3] / 1000) }' "$1" >"$2"
}
