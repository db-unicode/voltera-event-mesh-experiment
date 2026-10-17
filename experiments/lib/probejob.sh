#!/bin/sh
# Jobs de carga de E3, E4 y E7 con meter-sim (perfiles steady, probe y direct-positions). Se usa con `.`; no se ejecuta.
# Requiere experiments/lib/common.sh cargado antes.
#
# Imagen voltera/meter-sim:dev (PROBEJOB_IMAGE para otra): se reconstruye desde el árbol actual con la caché de Docker
# (rápido) y se carga en kind, así los perfiles probe y direct-positions siempre están.
#
#   probejob_build                          construye y carga la imagen
#   probejob_start <región> <modo> <args>   crea el Job y deja su nombre en PROBEJOB_LAST (no lo imprime: el contador
#                                           de Jobs debe vivir en el shell actual)
#   probejob_wait <región> <job> [seg]      espera al Job, imprime sus logs y devuelve 0 si terminó bien
#   probejob_run <región> <modo> <args>     start + wait
#   probejob_lines <PREFIJO>                filtra de stdin las líneas "PREFIJO ..." (PROBE, READS) y quita el prefijo
#   probejob_direct_secret <región>         crea el Secret e04-direct-rabbitmq (ver abajo)
#
# Modos:
#   amqp    credenciales del Secret meter-sim-rabbitmq (usuario meter-sim-<r>: write sobre meter-declarations.x).
#   probe   igual, más REDIS_PASSWORD del Secret market-view-credentials. Redis no tiene usuarios de solo lectura: el
#           perfil probe usa la contraseña del dueño de la vista, pero solo lee (HGET/HGETALL/SCAN). Es una herramienta
#           de medición, no parte del dominio.
#   direct  credenciales del Secret e04-direct-rabbitmq = las de declaration-gateway-<r> (único usuario con write sobre
#           energy-positions.x además del admin). Solo para E4: publica saltándose gateway y positions-db.
# PROBEJOB_DEADLINE (s, 900 por defecto) acota cada Job.

PROBEJOB_IMAGE=${PROBEJOB_IMAGE:-voltera/meter-sim:dev}
PROBEJOB_SEQ=0
PROBEJOB_LAST=

probejob_build() {
  need docker
  need kind
  docker build -q --build-arg CMD=meter-sim -t "$PROBEJOB_IMAGE" "$ROOT" >/dev/null || return 1
  kind load docker-image --name "$CLUSTER_NAME" "$PROBEJOB_IMAGE" >/dev/null || return 1
}

probejob_direct_secret() {
  _pw=$(k -n voltera-infra get secret rabbitmq-users -o json | jq -er --arg u "declaration-gateway-$1" '.data[$u]' | base64 -d) || return 1
  k -n "voltera-$1" create secret generic e04-direct-rabbitmq \
    --from-literal="AMQP_USER=declaration-gateway-$1" --from-literal="AMQP_PASSWORD=$_pw" \
    --dry-run=client -o yaml | k apply -f - >/dev/null
}

probejob_start() {
  _region=$1
  _mode=$2
  shift 2
  _ns=voltera-$_region
  PROBEJOB_SEQ=$((PROBEJOB_SEQ + 1))
  PROBEJOB_LAST=exp-load-$(date +%s)-$$-$PROBEJOB_SEQ
  _deadline=${PROBEJOB_DEADLINE:-900}
  _args=$(printf '"%s",' "$@")
  _args=${_args%,}
  case $_mode in
    amqp | probe) _secret=meter-sim-rabbitmq ;;
    direct) _secret=e04-direct-rabbitmq ;;
    *) die "probejob: modo desconocido '$_mode' (amqp|probe|direct)" ;;
  esac
  _redis=
  if [ "$_mode" = probe ]; then
    _redis='
            - name: REDIS_PASSWORD
              valueFrom: {secretKeyRef: {name: market-view-credentials, key: REDIS_PASSWORD}}'
  fi
  k -n "$_ns" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $PROBEJOB_LAST
  labels: {app: exp-load-job, experiment: e03-e04-e07}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $_deadline
  ttlSecondsAfterFinished: 900
  template:
    metadata:
      labels: {app: exp-load-job, experiment: e03-e04-e07}
    spec:
      serviceAccountName: meter-sim
      restartPolicy: Never
      containers:
        - name: meter-sim
          image: $PROBEJOB_IMAGE
          imagePullPolicy: IfNotPresent
          args: [$_args]
          env:
            - {name: REGION, value: $_region}
            - {name: GOMEMLIMIT, value: 100MiB}
            - name: AMQP_USER
              valueFrom: {secretKeyRef: {name: $_secret, key: AMQP_USER}}
            - name: AMQP_PASSWORD
              valueFrom: {secretKeyRef: {name: $_secret, key: AMQP_PASSWORD}}$_redis
          resources:
            requests: {cpu: 10m, memory: 32Mi}
            limits: {memory: 128Mi}
YAML
}

probejob_wait() {
  _ns=voltera-$1
  _job=$2
  _limit=${3:-$((${PROBEJOB_DEADLINE:-900} + 30))}
  _i=0
  _rc=
  while [ "$_i" -lt "$_limit" ]; do
    _st=$(k -n "$_ns" get job "$_job" -o jsonpath='{.status.succeeded}/{.status.failed}')
    case $_st in
      1/*) _rc=0; break ;;
      */1) _rc=1; break ;;
    esac
    _i=$((_i + 1))
    sleep 1
  done
  [ "$_rc" ] || _rc=1
  k -n "$_ns" logs "job/$_job" -c meter-sim 2>&1 || true
  return "$_rc"
}

probejob_run() {
  probejob_start "$@" || return 1
  probejob_wait "$1" "$PROBEJOB_LAST"
}

probejob_lines() { sed -n "s/^$1 //p"; }
