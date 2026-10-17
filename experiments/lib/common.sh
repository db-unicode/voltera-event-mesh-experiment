#!/bin/sh
# Helpers compartidos por tasks y spikes. Se usa con `.`; no se ejecuta.

# shellcheck disable=SC2034  # ROOT lo consumen los scripts que cargan este archivo
CLUSTER_NAME=${CLUSTER_NAME:-voltera}
KUBE_CONTEXT=kind-${CLUSTER_NAME}
ROOT=${MISE_PROJECT_ROOT:-$(git rev-parse --show-toplevel)}

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

die() {
  log "ERROR: $*"
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "falta '$1' (ejecuta: mise install)"
}

# kubectl fijado al contexto del cluster, sin depender del current-context
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }

# helm fijado al contexto del cluster
h() { helm --kube-context "$KUBE_CONTEXT" "$@"; }

# wait_rollout <kind/name> <namespace> [timeout]
wait_rollout() {
  log "esperando $2/$1"
  k -n "$2" rollout status "$1" --timeout="${3:-300s}"
}

# require_cluster: falla si el cluster no existe o no responde
require_cluster() {
  kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME" \
    || die "el cluster '$CLUSTER_NAME' no existe (ejecuta: mise run cluster:up)"
  k get nodes >/dev/null 2>&1 || die "el contexto $KUBE_CONTEXT no responde"
}
