# Reconstruye la cadena de un par SURPLUS+DEFICIT a partir de `expctl trace --json`.
# Entrada: {hops: [...]} con las líneas de log de los ids de las dos declaraciones, del matchId y del eventId del match.
# Variables: $d1 $d2 (eventId de las declaraciones), $match (matchId), $ev (eventId del match).
# Salida: la cadena de hops elegidos (con la declaración crítica, la que cierra el par), sus restricciones de orden,
# servicios, brokers y la descomposición de latencia.

def epoch:
  (sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)
  + ((capture("\\.(?<f>[0-9]+)Z$").f // "0") | ("0." + .) | tonumber);

.hops | map(. + {t: (.ts | epoch)}) as $H
| def pick($svc; $stage; ids):
    [$H[] | select(.service == $svc and .stage == $stage and (.eventId as $e | ids | index($e) != null))]
    | sort_by(.t) | .[0];
  def pick_match($svc; $stage):
    [$H[] | select(.service == $svc and .stage == $stage and ((.line.matchId // "") == $match))]
    | sort_by(.t) | .[0];

# la declaración crítica es la que el proyector aplica última: su aplicación cierra el par
([$H[] | select(.service == "market-projector" and .stage == "apply" and (.eventId == $d1 or .eventId == $d2))]
  | sort_by(.t) | .[-1].eventId) as $crit
| [$crit] as $c
| [$ev] as $e
| [
    {name: "sent",             broker: "rabbitmq", h: pick("meter-sim"; "publish"; $c), after: []},
    {name: "gateway_consume",  broker: "rabbitmq", h: pick("declaration-gateway"; "consume"; $c), after: ["sent"]},
    {name: "gateway_commit",   broker: "",         h: pick("declaration-gateway"; "commit"; $c), after: ["gateway_consume"]},
    {name: "gateway_publish",  broker: "rabbitmq", h: pick("declaration-gateway"; "publish"; $c), after: ["gateway_commit"]},
    {name: "projector_consume", broker: "rabbitmq", h: pick("market-projector"; "consume"; $c), after: ["gateway_commit"]},
    {name: "projector_apply",  broker: "",         h: pick("market-projector"; "apply"; $c), after: ["projector_consume"]},
    {name: "match_commit",     broker: "",         h: pick("matching-engine"; "commit"; $e), after: ["projector_apply"]},
    {name: "match_publish",    broker: "kafka",    h: pick("matching-engine"; "publish"; $e), after: ["match_commit"]},
    {name: "transfer_consume", broker: "kafka",    h: pick("energy-transfer"; "consume"; $e), after: ["match_commit"]},
    {name: "device_command",   broker: "",         h: pick_match("device-sim"; "command"), after: ["transfer_consume"]},
    {name: "transfer_command", broker: "",         h: pick("energy-transfer"; "command"; $e), after: ["device_command"]},
    {name: "bridge_consume",   broker: "kafka",    h: pick("mesh-bridge"; "consume"; $e), after: ["match_commit"]},
    {name: "bridge_publish",   broker: "nats",     h: pick("mesh-bridge"; "publish"; $e), after: ["bridge_consume"]},
    {name: "billing_received", broker: "nats",     h: pick("billing-sim"; "received"; $e), after: ["bridge_consume"]}
  ] as $defs
| ($defs | map(select(.h != null) | {key: .name, value: .h.t}) | from_entries) as $T
| ($defs | map(select(.h != null))) as $present
| ($T.sent // ($present[0].h.t)) as $t0
| ($present | map(. as $d | {
      name: $d.name, service: $d.h.service, stage: $d.h.stage, pod: $d.h.pod, eventId: $d.h.eventId, broker: $d.broker,
      ts: $d.h.ts, offset_s: ($d.h.t - $t0),
      violated: [$d.after[] | select(($T[.] // null) != null and ($T[.]) >= $d.h.t)]
    }) | sort_by(.offset_s)) as $chain
| ($chain | map(select((.violated | length) > 0))) as $bad
| (if ($T.billing_received // 0) >= ($T.device_command // 0) then "billing_received" else "device_command" end) as $end
| (if $end == "billing_received"
   then [["sent", "gateway_consume", "cold_start_gateway"], ["gateway_consume", "gateway_commit", "gateway_process"],
         ["gateway_commit", "projector_apply", "outbox_to_projection"], ["projector_apply", "match_commit", "match_window"],
         ["match_commit", "bridge_consume", "kafka_to_bridge"], ["bridge_consume", "billing_received", "nats_to_billing"]]
   else [["sent", "gateway_consume", "cold_start_gateway"], ["gateway_consume", "gateway_commit", "gateway_process"],
         ["gateway_commit", "projector_apply", "outbox_to_projection"], ["projector_apply", "match_commit", "match_window"],
         ["match_commit", "transfer_consume", "kafka_to_transfer"], ["transfer_consume", "device_command", "transfer_to_device"]]
   end
   | map(. as [$a, $b, $n] | {name: $n, from: $a, to: $b, seconds: (($T[$b] // null) as $tb | ($T[$a] // null) as $ta | if $ta == null or $tb == null then null else $tb - $ta end)})
  ) as $segs
| ($T[$end] - $t0) as $total
| {critical_declaration: $crit, end_hop: $end,
   chain: $chain,
   hops_present: ($chain | length),
   order_violations: ($bad | map({name, violated})),
   ordered: (($bad | length) == 0),
   services: ($chain | map(.service) | unique),
   brokers: ($chain | map(.broker) | map(select(. != "")) | unique),
   total_s: $total,
   segments: $segs,
   segments_sum_s: ($segs | map(.seconds // 0) | add),
   segments_error_pct: (if $total > 0 then ((($segs | map(.seconds // 0) | add) - $total) | fabs) / $total * 100 else null end),
   branch_s: {transfer: (if $T.device_command and $T.sent then $T.device_command - $T.sent else null end),
              mesh: (if $T.billing_received and $T.sent then $T.billing_received - $T.sent else null end)}}
