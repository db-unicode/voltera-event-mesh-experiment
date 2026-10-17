#!/bin/sh
# Runner genérico de experimentos: run.sh <exp-id> [rep]
#   <exp-id> es el nombre de la carpeta experiments/<id>/ o su prefijo (e00 -> e00-harness).
#   La carpeta define run.sh (funciones drive y collect; opcionales exp_reset y exp_warmup)
#   y criteria.yaml. Fases: snapshot -> reset -> warmup -> drive -> collect -> assert -> plot.
# Evidencia: experiments/results/<exp>/<UTC>-<sha>[-dirty]-r<rep>/ (+ symlink latest).
# Sale con 1 si algún criterio falla o drive/collect fallan.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/lib/common.sh"
# shellcheck source=experiments/lib/expctl.sh
. "$HERE/lib/expctl.sh"
# shellcheck source=experiments/lib/chaos.sh
. "$HERE/lib/chaos.sh"

[ "$#" -ge 1 ] || die "uso: run.sh <exp-id> [rep]"
id_in=$1
rep=${2:-1}
case "$id_in" in */* | .*) die "exp-id inválido: $id_in" ;; esac
case "$rep" in '' | *[!0-9]*) die "rep debe ser un entero: $rep" ;; esac

need kubectl
need jq
need git
need go
require_cluster

# resuelve la carpeta del experimento
exp=
if [ -d "$HERE/$id_in" ]; then
  exp=$id_in
else
  for d in "$HERE/$id_in"-*/; do
    [ -d "$d" ] || continue
    [ -z "$exp" ] || die "exp-id ambiguo: $id_in"
    exp=$(basename "$d")
  done
fi
[ -n "$exp" ] && [ -f "$HERE/$exp/run.sh" ] || die "no existe experiments/$id_in*/run.sh"
[ -f "$HERE/$exp/criteria.yaml" ] || die "falta experiments/$exp/criteria.yaml"

# params.env se exporta: lo ven drive/collect y la expansión ${VAR} de criteria.yaml
set -a
# shellcheck source=experiments/params.env
. "$HERE/params.env"
set +a

expctl_build

sha=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD || sha=${sha}-dirty
OUT="$HERE/results/$exp/$(date -u +%Y%m%dT%H%M%SZ)-$sha-r$rep"
mkdir -p "$OUT/raw" "$OUT/prom" "$OUT/logs" "$OUT/figures"
EXP_ID=$exp
EXP_REP=$rep
export OUT EXP_ID EXP_REP
log "$exp r$rep: evidencia en ${OUT#"$ROOT"/}"

sh "$HERE/lib/env-snapshot.sh" "$HERE/params.env" >"$OUT/env.json" || die "falló env-snapshot"
cp "$HERE/params.env" "$OUT/params.env"

# shellcheck source=/dev/null
. "$HERE/$exp/run.sh"
type drive >/dev/null 2>&1 || die "experiments/$exp/run.sh no define drive()"
type collect >/dev/null 2>&1 || die "experiments/$exp/run.sh no define collect()"

rc=0
if type exp_reset >/dev/null 2>&1; then
  log "fase reset"
  exp_reset || { log "exp_reset falló"; rc=1; }
fi
if type exp_warmup >/dev/null 2>&1; then
  log "fase warmup"
  exp_warmup || { log "exp_warmup falló"; rc=1; }
fi

log "fase drive"
t_start=$(expctl now)
drive || { log "drive falló"; rc=1; }
t_end=$(expctl now)
printf '{"tStart":"%s","tEnd":"%s"}\n' "$t_start" "$t_end" >"$OUT/window.json"

log "fase collect"
collect "$t_start" "$t_end" "$OUT" || { log "collect falló"; rc=1; }

log "fase assert"
expctl assert --criteria "$HERE/$exp/criteria.yaml" --data "$OUT" --out "$OUT/result.json" || rc=1

for csv in "$OUT"/*.csv "$OUT"/raw/*.csv; do
  [ -f "$csv" ] || continue
  name=$(basename "$csv" .csv)
  expctl plot --csv "$csv" --x ts --title "$exp $name" --out "$OUT/figures/$name.png" \
    || log "sin figura para ${csv#"$OUT"/}"
done

ln -sfn "$(basename "$OUT")" "$HERE/results/$exp/latest"
if [ "$rc" -eq 0 ]; then
  log "$exp r$rep: PASS (${OUT#"$ROOT"/})"
else
  log "$exp r$rep: FAIL (${OUT#"$ROOT"/})"
fi
exit "$rc"
