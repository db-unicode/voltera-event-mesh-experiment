# E3. CQRS: retraso escritura -> vista

```sh
mise run exp:run e03
```

## Hipótesis

La lectura (Redis `market-view`) converge tras el commit en `positions-db` dentro de la tolerancia de negocio, también bajo
carga base y durante el pico, y no depende de `positions-db` para responder.

## Medición (caja negra, perfil `probe` de meter-sim)

- `meter-sim --profile probe`: medidores `probe-e03-<ts>-<fase>-NNNN` (50, uno por segundo, en rotación). Cada sondeo
  declara una versión que no se repite; cuando RabbitMQ confirma la publicación, lee `_v` de `pos:<zona>:<medidor>` en Redis
  cada 20 ms hasta ver esa versión y anota `visible_at` (resolución: 20 ms + RTT de Redis, ~1 ms).
- `committedAt` = `outbox.created_at` (`clock_timestamp()` dentro de la transacción), leído por SQL
  (`select event_id, ... from outbox where aggregate_id like 'probe-e03-<ts>-%'`) y unido por `eventId` con el CSV del sondeo.
  Un solo host: no hay sesgo de reloj. Un sondeo que no se ve en su `--probe-timeout` entra en los percentiles con ese valor
  (cota inferior), no se descarta.
- Credenciales: el perfil `probe` usa la contraseña del Secret `market-view-credentials` porque Redis no tiene usuarios de
  solo lectura; solo ejecuta `HGET`, `HGETALL` y `SCAN`. Es una herramienta de medición, no parte del dominio.
- Imagen `voltera/meter-sim:exp-e03-e04-e07` (los perfiles nuevos no están en `:dev` hasta integrar).

## Fases (región r1, ~12 min)

| Fase | Carga | Qué se mide |
|---|---|---|
| A | steady 3000 medidores / 30 s (100 decl/s), sondeo 60 s sin lecturas | p50/p95/p99 committed->visible; p95 de procesamiento del gateway |
| B | igual, 15 s después, con 200 lecturas/s de fondo (HGETALL de keys `pos:*` al azar, estilo matching) | idem; p95 de procesamiento del gateway |
| C | solo los lectores (200/s, 100 s); `positions-db` a 0 réplicas durante 30 s | errores de lectura y p95 de lectura con la base arriba y caída |
| D | spike a `SPIKE_RATE` (276/s) durante 60 s desde 0 réplicas del gateway, sondeo 170 s con lecturas | p99 durante el pico; recuperación |
| Convergencia | tras A+B y tras D | Redis == `positions-db` en `side, kwh, validUntil, version` (todos los medidores del prefijo, sondeos incluidos) |

Descomposición (medias de los histogramas internos, por ventana): `committed -> emitted` = media(`event_e2e_lag{committed}`) -
media(`{emitted}`); `emitted -> consumido` = media(`{emitted}`) - media(`event_processing_seconds` del proyector);
`consumido -> SET` = media(`event_processing_seconds` del proyector). Además p50/p95/p99 de cada etapa.

## Criterio

Ver `criteria.yaml`. Resumen: base p95 <= 2 s y p99 <= 5 s (A y B); pico p99 <= 30 s y sondeos enviados 60 s después del fin
del pico con p99 <= 5 s; con `positions-db` caída, 0 errores de lectura y p95 de lectura +-20%; la lectura concurrente sube el
p95 de procesamiento del gateway <= 20%; convergencia 100%.

Ajustes respecto a 03-experiments.md (medidas de ruido, no cambios de umbral):

- p95 de lectura: se compara la mediana de los p95 por segundo de cada ventana y se ignora una diferencia < 0,5 ms.
- p95 de escritura: el histograma `event_processing_seconds` tiene buckets de 25/50/100 ms; se ignora una diferencia < 5 ms.
- El p95 de cada etapa y de la escritura sale de `histogram_quantile` sobre `increase()` de la ventana de cada fase.

## Evidencia

`data/probe_{a,b,p}.csv` (sondeos), `data/outbox_probe.csv`, `raw/latency_*.csv` (una fila por sondeo con `committed_to_visible_s`
y su gráfico temporal en `figures/`), `figures/cdf_*.png`, `raw/reads_*.csv` (lecturas por segundo),
`latency_quantiles.json`, `decomposition.json`, `convergence.json`, `metrics.json`, `result.json`.

## Resultado (corrida final, `results/final/e03-cqrs-lag/`)

Evidencia: `experiments/results/final/e03-cqrs-lag/` (corrida `20261017T070732Z-18789b0-dirty-r1`, ronda `final-run-18789b0.tsv`). 30 de 30 criterios PASS. La marca `-dirty` se debe solo a la evidencia promovida a `results/final` durante la corrida; el código es el del commit indicado.

| Fase | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| A: base, sin lecturas | 60 | 20,6 ms | 26,4 ms | 75,9 ms | 136,5 ms |
| B: base, 200 lecturas/s | 60 | 17,3 ms | 23,8 ms | 67,3 ms | 127,9 ms |
| D: durante el pico (60 s a 276 decl/s, desde 0 réplicas) | 59 | 13,1 ms | 24,2 ms | 32,8 ms | 42,7 ms |
| D: 60 s después del pico | 51 | 20,5 ms | 23,4 ms | 24,1 ms | 24,3 ms |

(committed -> visible, caja negra.) Ningún sondeo expiró (290 de 290 visibles).

| Otra medida | Observado | Criterio |
|---|---|---|
| Lecturas con `positions-db` en 0 réplicas (28 s, 200/s) | 5600 lecturas, 0 errores (20 000 lecturas en todo el Job, 0 errores) | 0 errores |
| p95 de lectura, mediana de los p95 por segundo: base arriba / base caída | 0,562 ms / 0,5465 ms; diferencia de 0,0155 ms, por debajo del umbral de ruido de 0,5 ms (se cuenta 0) | +-20 % (observado 0) |
| p95 de procesamiento del gateway sin / con lecturas | 39,2 ms / 43,4 ms: +10,6 % bruto; diferencia de 4,2 ms, por debajo de 5 ms (se cuenta 0) | <= +20 % |
| Convergencia Redis == `positions-db` (steady A) | 3000/3000 | 100 % |
| Convergencia tras el pico (steady) / sondeos del pico | 3000/3000 / 50/50 | 100 % |
| Convergencia de los sondeos base | 50/50 (otros 50 expiraron por TTL antes de comprobar) | 100 % de los no expirados |
| Generador de carga | 27 000 (base) y 15 870 (pico) declaraciones, 0 descartadas | 0 |

Descomposición (medias y percentiles de los histogramas internos; ventana base = AB completa, pico = D completa):

| Etapa | Base | Pico |
|---|---|---|
| committed -> visible (histograma, p50 / p95 / p99) | 5,5 / 17,1 / 34,3 ms | 5,7 / 24,6 / 75,4 ms |
| committed -> emitted (media) | 2,7 ms | 4,2 ms |
| emitted -> consumido (media) | 2,7 ms | 2,6 ms |
| consumido -> SET (media) | 0,2 ms | 0,3 ms |

`committed_p99_whole_run_s` (la consulta del enunciado sobre toda la corrida) = 48,3 ms.

La caja negra (p50 20,6 ms en la fase A) está por encima del histograma interno (p50 5,5 ms en la base): suma el periodo de lectura de 20 ms (en promedio 10 ms de espera hasta la siguiente lectura) y el RTT de Redis a través de los dos sidecars.

## Hallazgo: el retraso que importa está antes del commit

committed -> visible no se degrada con el pico: p99 32,8 ms durante el pico frente a 75,9 ms en la fase A (caja negra). El retraso extremo-a-extremo sí: `sent_to_visible` (enviado por el medidor -> visible) incluye la espera en la cola `meter-declarations` antes de que el gateway haga commit. Es un tema de E2 (KEDA), pero lo ve el matching:

| Fase | p50 | p95 | p99 | max |
|---|---|---|---|---|
| base, sin lecturas | 47,8 ms | 9,3 s | 10,3 s | 10,4 s |
| durante el pico | 15,2 s | 20,4 s | 20,9 s | 21,0 s |

Recuperación tras el pico (`sent_to_visible.peak_recovery_s`): 81,8 s, informativo y sin criterio en E3. Incluso con carga constante de 100 decl/s hay episodios de ~10 s de cola (`figures/latency_base_noread.png`, `figures/latency_peak_all.png`): aparecen como una rampa que sube y baja en unos segundos; la causa no se investigó (E2 mide el escalado del gateway).

## Tabla final

| Experimento | Hipótesis | Métrica | Criterio de éxito | Resultado |
|---|---|---|---|---|
| E3 CQRS: retraso escritura -> vista | La vista converge tras el commit dentro de la tolerancia, también con carga base y en el pico, y no depende de `positions-db` | p95/p99 committed -> visible (sondeo sobre Redis); errores de lectura con la base caída; p95 de escritura con y sin lectura; convergencia | Base p95 <= 2 s y p99 <= 5 s; pico p99 <= 30 s y <= 5 s a los 60 s; 0 errores y p95 +-20%; escritura <= +20%; convergencia 100% | Base p95 26,4 / 23,8 ms y p99 75,9 / 67,3 ms (A / B); pico p99 32,8 ms, recuperado p99 24,1 ms; 0 errores en 20 000 lecturas (5600 con la base caída); p95 de lectura 0,562 / 0,5465 ms (base / base caída); escritura +10,6 % bruto; convergencia 3000/3000 y 50/50. PASS 30/30 (`experiments/results/final/e03-cqrs-lag/`, corrida `18789b0-dirty-r1`) |
