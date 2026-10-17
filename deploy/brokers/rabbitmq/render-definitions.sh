#!/bin/sh
# Init container: rellena definitions.json.tmpl con las contraseñas del Secret.
# /secrets/<usuario> contiene la contraseña; en la plantilla aparece como @@<usuario>@@ (solo hex: sin escapes en sed).
set -eu
cp /template/definitions.json.tmpl /out/definitions.json
for f in /secrets/*; do
  name=$(basename "$f")
  pass=$(cat "$f")
  sed "s|@@${name}@@|${pass}|g" /out/definitions.json >/out/definitions.tmp
  mv /out/definitions.tmp /out/definitions.json
done
if grep -q '@@' /out/definitions.json; then
  echo "quedaron contraseñas sin sustituir:" >&2
  grep -o '@@[^@]*@@' /out/definitions.json >&2
  exit 1
fi
