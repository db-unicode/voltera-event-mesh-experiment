#!/bin/sh
# Crea (o actualiza) el stream BILLING_MATCHED y el consumer durable billing. Idempotente.
# stream.cfg y consumer.cfg son JSON (la extensión evita que kubeconform los trate como manifiestos).
set -eu

i=0
until nats account info >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -lt 60 ] || { echo "nats-global no responde"; exit 1; }
  sleep 2
done

if nats stream info BILLING_MATCHED >/dev/null 2>&1; then
  nats stream edit BILLING_MATCHED --config /config/stream.cfg --force
else
  nats stream add --config /config/stream.cfg
fi

if nats consumer info BILLING_MATCHED billing >/dev/null 2>&1; then
  nats consumer edit BILLING_MATCHED billing --config /config/consumer.cfg --force
else
  nats consumer add BILLING_MATCHED --config /config/consumer.cfg
fi

nats stream info BILLING_MATCHED
nats consumer info BILLING_MATCHED billing
