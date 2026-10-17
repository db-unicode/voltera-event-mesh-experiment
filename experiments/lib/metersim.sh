#!/bin/sh
# Helper para correr meter-sim como Job dentro del mesh (generador de carga de S11/S16). Se usa con `.`; no se ejecuta.
# Requiere experiments/lib/common.sh cargado antes. Usa el ServiceAccount y el Secret meter-sim-rabbitmq que crea
# `mise run apps:write-side` en el namespace de la región.

METERSIM_JOB_SEQ=0

# metersim_run <region> <args de meter-sim...>
# Corre meter-sim en un Job de voltera-<region>, imprime su salida (logs y, en la última línea JSON, el resumen) y
# devuelve 0 si el Job terminó bien. METERSIM_DEADLINE (s, por defecto 600) acota el Job.
metersim_run() {
  _region=$1
  shift
  _ns=voltera-$_region
  METERSIM_JOB_SEQ=$((METERSIM_JOB_SEQ + 1))
  _job=meter-sim-$(date +%s)-$$-$METERSIM_JOB_SEQ
  _deadline=${METERSIM_DEADLINE:-600}
  _args=$(printf '"%s",' "$@")
  _args=${_args%,}
  k -n "$_ns" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $_job
  labels: {app: meter-sim-job}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $_deadline
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: {app: meter-sim-job}
    spec:
      serviceAccountName: meter-sim
      restartPolicy: Never
      containers:
        - name: meter-sim
          image: voltera/meter-sim:dev
          imagePullPolicy: IfNotPresent
          args: [$_args]
          env:
            - {name: REGION, value: $_region}
            - name: AMQP_USER
              valueFrom: {secretKeyRef: {name: meter-sim-rabbitmq, key: AMQP_USER}}
            - name: AMQP_PASSWORD
              valueFrom: {secretKeyRef: {name: meter-sim-rabbitmq, key: AMQP_PASSWORD}}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {memory: 128Mi}
YAML
  _i=0
  _rc=
  while [ "$_i" -lt $((_deadline / 2 + 30)) ]; do
    _st=$(k -n "$_ns" get job "$_job" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $_st in
      1/*) _rc=0; break ;;
      */1) _rc=1; break ;;
    esac
    _i=$((_i + 1))
    sleep 2
  done
  [ "$_rc" ] || _rc=1
  k -n "$_ns" logs "job/$_job" -c meter-sim 2>&1 || true
  return "$_rc"
}

# metersim_summary: lee de stdin la salida de metersim_run y deja solo el resumen JSON
metersim_summary() { grep '^{"profile"' | tail -n 1; }
