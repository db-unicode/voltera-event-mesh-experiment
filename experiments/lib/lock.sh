#!/bin/sh
# Lock de cluster para corridas de experimentos: un solo experimento a la vez (mkdir es atómico en POSIX).
# Uso: `. experiments/lib/lock.sh; cluster_lock <dueño>; ...; cluster_unlock` (el trap libera al salir).

CLUSTER_LOCK_DIR=${CLUSTER_LOCK_DIR:-${TMPDIR:-/tmp}/voltera-cluster.lock}

cluster_lock() {
  _owner=${1:-$$}
  _waited=0
  until mkdir "$CLUSTER_LOCK_DIR" 2>/dev/null; do
    if [ $((_waited % 30)) -eq 0 ]; then
      printf 'esperando lock del cluster (dueño: %s)\n' "$(cat "$CLUSTER_LOCK_DIR/owner" 2>/dev/null || echo '?')" >&2
    fi
    _waited=$((_waited + 5))
    sleep 5
  done
  printf '%s %s\n' "$_owner" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$CLUSTER_LOCK_DIR/owner"
  trap cluster_unlock EXIT INT TERM
}

cluster_unlock() {
  rm -rf "$CLUSTER_LOCK_DIR"
}
