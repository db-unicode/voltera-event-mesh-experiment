# Arnés de experimentos

Resultados de la corrida final (12 experimentos) y cómo reproducirlos: [RESULTS.md](RESULTS.md).

```sh
mise run exp:run <id> [rep]   # p. ej. exp:run e00 2; id = carpeta experiments/<id>*/
mise run exp:e00              # alias de exp:run e00-harness
mise run exp:report           # experiments/results/summary.md
mise run spike:s12            # primitivas de caos
```

`expctl` se compila a `bin/expctl` (ignorado por git) en cada task. Corre en el host y usa `kubectl --context $KUBE_CONTEXT` (por defecto `kind-voltera`); los port-forward usan puertos 19700-19799 y se cierran solos. Con mTLS STRICT funcionan también contra pods con sidecar (entran por el localhost del pod).

## Estructura

| Ruta | Contenido |
|---|---|
| `params.env` | Parámetros globales (`KEY=value`, `C_POD=46`); el runner los exporta |
| `run.sh` | Runner: `run.sh <id> [rep]` |
| `<id>/run.sh` | Define `drive` y `collect <t_start> <t_end> <out>`; opcionales `exp_reset` y `exp_warmup` |
| `<id>/criteria.yaml` | Criterios que evalúa `expctl assert` |
| `lib/chaos.sh` | `chaos_kill`, `chaos_down/up`, `chaos_pause_keda/resume_keda`, `chaos_failpoint`, `chaos_partition/heal`, `wait_ready` |
| `lib/env-snapshot.sh` | Imprime `env.json` |
| `results/<id>/<UTC>-<sha>[-dirty]-r<rep>/` | `env.json`, `params.env`, `window.json`, `raw/`, `prom/`, `logs/`, `figures/`, `result.json`; `latest` apunta a la última |

Fases del runner: snapshot, `exp_reset`, `exp_warmup`, `drive`, `collect`, `assert`, `plot` (una figura por cada CSV en la raíz y en `raw/`), `ln -sfn latest`. Sale con 1 si falla un criterio, `drive` o `collect`. Dentro de `drive`/`collect` hay que escribir `cmd || return 1`: el runner las invoca con `||` y `set -e` no aplica.

El runner define `OUT`, `EXP_ID`, `EXP_REP` (y `HERE`, `ROOT`, `EXPCTL`), y deja cargadas las funciones `expctl`, `k`, `h`, `log`, `die` y las de `lib/chaos.sh`.

## criteria.yaml

Lista de criterios (o un mapa con la clave `criteria:`). `${VAR}` se expande con el entorno (los de `params.env` están disponibles); una variable sin definir es un error.

```yaml
- id: lost                         # único
  description: Sin pérdida de matches
  metric: "metrics.json:$.lost"    # archivo (relativo a la corrida) : expresión
  op: eq                           # le | lt | ge | gt | eq
  value: 0
- id: peak
  metric: "raw/replicas.csv:max(replicas)"
  op: le
  value: ${KEDA_MAX_REPLICAS}
```

Expresiones:

- JSON: JSONPath simple (`$.a.b`, `$.a[0]`, `$.a[-1]`, `$['k.y']`). `func($.array)` aplica una función a un array numérico.
- CSV: `func(columna)`. Funciones: `max, min, last, first, count, sum, avg, p50, p95, p99` (y `pNN`). Ignora celdas vacías; los percentiles interpolan linealmente.

`result.json` = `{verdict, data, criteria: [{id, description, metric, observed, op, expected, verdict, error?}]}`. Una métrica ilegible o un archivo ausente da FAIL con `error`.

## expctl

| Comando | Ejemplo |
|---|---|
| `now` | `expctl now` imprime `2026-10-16T10:00:00.123456789Z` (9 decimales) |
| `elapsed <start> [end]` | `expctl elapsed "$t0" "$t1"` imprime segundos |
| `prom query` | `expctl prom query 'sum(up)' --time -1m` |
| `prom range` | `expctl prom range 'up' --start "$t0" --end "$t1" --step 1s --out up.json` |
| `sample` | `expctl sample --interval 2s --duration 120s --out s.csv --replicas gw=voltera-r1/declaration-gateway --prom depth='rabbitmq_detailed_queue_messages_ready{queue="meter-declarations"}'` |
| `http-get` | `expctl http-get voltera-r1/device-sim:9090/state`; `--method POST` y `ns/pod/<pod>:9090/...` también |
| `sql` | `expctl sql voltera-r1/matches-db "select count(*) from matches" [--json]` |
| `quantile` | `expctl quantile --file s.csv --col depth --q 0.5,0.95,0.99` |
| `trace` | `expctl trace <eventId> [--namespaces voltera-r1,voltera-global] [--since 10m] [--out t.json] [--json]` |
| `assert` | `expctl assert --criteria criteria.yaml --data <dir> --out result.json` |
| `plot` | `expctl plot --csv s.csv --x ts [--y a,b] --out fig.png` (`.svg` también) |
| `report` | `expctl report --results experiments/results --out experiments/results/summary.md` |

- `sample`: columnas `ts`, y por cada `--replicas [name=]ns/deploy` (o `ns/statefulset/nombre`) las columnas `name` (`spec.replicas`) y `name_ready` (`status.readyReplicas`); luego una por `--prom name=<promql>` (primer valor del vector, vacío si no hay serie). Una celda que falla queda vacía y se avisa por stderr.
- `sql`: usa el Secret `<statefulset>-credentials` y `kubectl exec statefulset/<n> -c postgres -- psql --csv`; la salida lleva cabecera (`--no-header` la quita).
- `trace`: lee `kubectl logs --timestamps` de todos los contenedores salvo los de Istio (más `--previous` si hubo reinicios) y se queda con las líneas JSON que contienen el id. Ordena por `ts` (o por el timestamp de kubectl si falta). Ignora líneas que no son JSON. Varios ids se aceptan como argumentos.
