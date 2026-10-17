#!/bin/sh
# Compila y expone expctl (bin/expctl). Se usa con `.`; no se ejecuta.
# Requiere haber cargado experiments/lib/common.sh (ROOT, need, die).

EXPCTL=${EXPCTL:-$ROOT/bin/expctl}
export EXPCTL
export KUBE_CONTEXT

# expctl_build: go build incremental de cmd/expctl
expctl_build() {
  need go
  (cd "$ROOT" && go build -o bin/expctl ./cmd/expctl) || die "falló go build ./cmd/expctl"
}

# expctl <args...>: ejecuta el binario compilado
expctl() { "$EXPCTL" "$@"; }
