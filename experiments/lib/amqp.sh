#!/bin/sh
# Helpers para correr amqp-probe dentro del mesh (Job con sidecar). Se usa con `.`; no se ejecuta.
# Requiere experiments/lib/common.sh cargado antes.

AMQP_HOST=${AMQP_HOST:-rabbitmq.voltera-infra.svc}
AMQP_JOB_SEQ=0

# amqp_copy_secret <namespace>: copia el Secret rabbitmq-users (claves = usuarios) al namespace
amqp_copy_secret() {
  k -n voltera-infra get secret rabbitmq-users -o json \
    | jq '{apiVersion, kind, type, metadata: {name: .metadata.name}, data}' \
    | k -n "$1" apply -f - >/dev/null
}

# amqp_run <namespace> <usuario> <vhost> <args de amqp-probe...>
# Corre amqp-probe en un Job con sidecar, imprime su salida y devuelve 0 si el Job terminó bien.
# La contraseña viene del Secret rabbitmq-users (clave = nombre del usuario).
amqp_run() {
  _ns=$1
  _user=$2
  _vhost=$3
  shift 3
  AMQP_JOB_SEQ=$((AMQP_JOB_SEQ + 1))
  _job=probe-$(date +%s)-$$-$AMQP_JOB_SEQ
  _args=$(printf '"%s",' "$@")
  _args=${_args%,}
  k -n "$_ns" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $_job
  labels: {app: amqp-probe}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 240
  template:
    metadata:
      labels: {app: amqp-probe}
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: voltera/amqp-probe:dev
          args: [$_args]
          env:
            - {name: AMQP_HOST, value: $AMQP_HOST}
            - {name: AMQP_USER, value: $_user}
            - {name: AMQP_VHOST, value: $_vhost}
            - name: AMQP_PASSWORD
              valueFrom:
                secretKeyRef: {name: rabbitmq-users, key: $_user}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {memory: 128Mi}
YAML
  _i=0
  while [ "$_i" -lt 120 ]; do
    _st=$(k -n "$_ns" get job "$_job" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $_st in
      1/*) _rc=0; break ;;
      */1) _rc=1; break ;;
    esac
    _i=$((_i + 1))
    sleep 2
  done
  [ "${_rc:-}" ] || _rc=1
  k -n "$_ns" logs "job/$_job" -c probe 2>&1 || true
  return "$_rc"
}
