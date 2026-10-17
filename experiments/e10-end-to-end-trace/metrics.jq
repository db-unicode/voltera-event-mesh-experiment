# Métricas de E10 a partir de la cadena (analyze.jq), los pods del arranque en frío y los eventos de k8s.
def epoch:
  (sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)
  + ((capture("\\.(?<f>[0-9]+)Z$").f // "0") | ("0." + .) | tonumber);

# Arranque en frío entre el instante $from y el primer mensaje consumido $to (epoch). Las anclas de k8s tienen resolución de
# 1 s: se acotan para que sean monótonas entre $from y $to; el error compara la suma de componentes con $to - $from.
def cold($from; $pod; $app; $to):
  ($pod.created | epoch) as $created
  | (if $app == null then $created else ($app.ts | epoch) end) as $started
  | ([$created, $from] | max) as $a2
  | ([$started, $a2] | max) as $a3
  | ([$to, $a3] | max) as $a4
  | [{name: "keda_detect_and_scale", seconds: ($a2 - $from)},
     {name: "pod_start_to_app_ready", seconds: ($a3 - $a2)},
     {name: "app_ready_to_first_message", seconds: ($a4 - $a3)}] as $parts
  | {total_s: ($to - $from), components: $parts, components_sum_s: ($a4 - $from),
     error_pct: (if ($to - $from) > 0 then ((($a4 - $from) - ($to - $from)) | fabs) / ($to - $from) * 100 else null end),
     pod: $pod.pod, pod_created: $pod.created, pod_ready: $pod.ready};

$C[0] as $c
| ($c.chain | map({key: .name, value: (.ts | epoch)}) | from_entries) as $T
| {run: $pfx, prefix: $pfx, sent_at: $send, done_at: $doneat,
   ids: {declaration_surplus: $d1, declaration_deficit: $d2, critical_declaration: $c.critical_declaration, match: $match, match_event: $ev},
   pair_exact: (if $M[0].rows == 1 and ($M[0].seller | startswith($pfx)) and ($M[0].buyer | startswith($pfx)) then 1 else 0 end),
   matches_for_prefix: $M[0].rows,
   hops_present: $c.hops_present,
   ordered: (if $c.ordered then 1 else 0 end),
   order_violations: $c.order_violations,
   services: $c.services, services_count: ($c.services | length),
   brokers: $c.brokers, brokers_count: ($c.brokers | length),
   total_s: $c.total_s, end_hop: $c.end_hop, branch_s: $c.branch_s,
   segments: $c.segments, segments_sum_s: $c.segments_sum_s, segments_error_pct: $c.segments_error_pct,
   cold_start: {gateway: cold($T.sent; $G[0]; $GA[0]; $T.gateway_consume),
                transfer: cold($T.match_commit; $TP[0]; $TA[0]; $T.transfer_consume)},
   k8s_events: {count: ($EV[0] | length), reasons: ($EV[0] | map(.reason) | unique),
                keda_activation: ($EV[0] | map(select(.reason == "KEDAScaleTargetActivated")) | length),
                pods_started: ($EV[0] | map(select(.reason == "Started")) | length)}}
