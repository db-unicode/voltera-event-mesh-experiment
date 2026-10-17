#!/bin/sh
# Imprime env.json (stdout): versiones de herramientas, helm ls -A, imágenes en uso con digest,
# git sha y estado sucio, y params. Uso: env-snapshot.sh [params.env]
# Variables opcionales: EXP_ID, EXP_REP.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=experiments/lib/common.sh
. "$HERE/common.sh"

need jq
need git
need kubectl

params=${1:-$ROOT/experiments/params.env}

# tool_version <comando...>: primera línea de la salida o "unknown"
tool_version() {
  v=$("$@" 2>/dev/null | head -n 1) || v=""
  printf '%s' "${v:-unknown}"
}

dirty=false
git -C "$ROOT" diff --quiet HEAD || dirty=true
sha=$(git -C "$ROOT" rev-parse --short HEAD)
branch=$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)

params_json=$(
  sed -e 's/[[:space:]]*#.*$//' -e '/^[[:space:]]*$/d' "$params" \
    | jq -R -s 'split("\n") | map(select(length > 0) | capture("^(?<k>[A-Za-z_][A-Za-z0-9_]*)=(?<v>.*)$")) | map({(.k): .v}) | add // {}'
)

helm_json=$(h ls -A -o json 2>/dev/null || echo '[]')
images_json=$(
  k get pods -A -o json 2>/dev/null \
    | jq '[.items[] | (.status.containerStatuses // [])[], (.status.initContainerStatuses // [])[] | {image, imageID}] | unique' \
    || echo '[]'
)
server=$(k version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion // "unknown"' || echo unknown)

jq -n \
  --arg id "${EXP_ID:-}" \
  --arg rep "${EXP_REP:-}" \
  --arg startedAt "$("$ROOT/bin/expctl" now 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg sha "$sha" --arg branch "$branch" --argjson dirty "$dirty" \
  --arg kubectl "$(kubectl version --client -o json 2>/dev/null | jq -r '.clientVersion.gitVersion // "unknown"' || echo unknown)" \
  --arg kind "$(tool_version kind version)" \
  --arg istioctl "$(tool_version istioctl version --remote=false)" \
  --arg helm "$(tool_version helm version --short)" \
  --arg context "$KUBE_CONTEXT" --arg server "$server" \
  --argjson helmLs "$helm_json" --argjson images "$images_json" --argjson params "$params_json" \
  '{
    experiment: {id: $id, rep: (if $rep == "" then null else ($rep | tonumber) end)},
    startedAt: $startedAt,
    git: {sha: $sha, branch: $branch, dirty: $dirty},
    tools: {kubectl: $kubectl, kind: $kind, istioctl: $istioctl, helm: $helm},
    cluster: {context: $context, serverVersion: $server},
    helm: $helmLs,
    images: $images,
    params: $params
  }'
