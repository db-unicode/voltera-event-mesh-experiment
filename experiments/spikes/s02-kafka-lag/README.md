# S2. Lag de Kafka en Prometheus

`kafka-exporter` (danielqsj v1.10.0) expone el lag por grupo y Prometheus lo recoge por anotación (`prometheus.io/scrape|port|path` en el pod; Istio fusiona el puerto de la app en el 15020, fuera del mTLS).

## Ejecutar

```sh
mise run brokers:kafka && mise run spike:s02
```

## Flujo

1. `seek --to end` deja el grupo `s02` con offsets confirmados al final de `r2.energy-matched`. Sin offsets confirmados el exporter no emite series del grupo; así el lag parte de 0 y no depende de corridas anteriores.
2. Se espera lag 0 en Prometheus, se producen 5000 eventos sin consumidor y se mide cuánto tarda el lag en llegar a 5000 (tolerancia 1 %, límite 30 s).
3. `consume` del grupo `s02` (commit manual) lee 5000/5000 y el lag vuelve a 0.

Usa `r2.energy-matched` para dejar `r1.energy-matched` limpio para S3.

## Nombres exactos de las series (kafka-exporter v1.10.0, verificados con curl)

| Serie | Etiquetas | Significado |
|---|---|---|
| `kafka_consumergroup_lag` | `consumergroup, topic, partition` | lag por partición |
| `kafka_consumergroup_lag_sum` | `consumergroup, topic` | lag del topic (suma de particiones) |
| `kafka_consumergroup_current_offset` | `consumergroup, topic, partition` | offset confirmado |
| `kafka_consumergroup_current_offset_sum` | `consumergroup, topic` | suma de offsets confirmados |
| `kafka_consumergroup_members` | `consumergroup` | miembros del grupo |
| `kafka_topic_partition_current_offset` | `topic, partition` | offset final del log |
| `kafka_topic_partition_oldest_offset` | `topic, partition` | offset inicial |
| `kafka_topic_partitions` | `topic` | número de particiones |

Query usada por el spike: `sum(kafka_consumergroup_lag{consumergroup="s02",topic="r2.energy-matched"})`. La etiqueta se llama `consumergroup` (sin guion bajo) y Prometheus no añade prefijo de job a estas series.

El exporter calcula el lag en cada petición; la latencia visible en Prometheus es el intervalo de scrape (15 s en el addon de Istio). Con commit manual tras procesar no se sobreestima el lag.

## Resultados

Ver `experiments/results/s02/<UTC>-<sha>/result.txt` (PASS/FAIL por check, con los segundos en `checks/<n>.log`), `exporter-series.txt` (salida de curl) y `exporter-metrics.txt`.

Medido: el lag llega a 5000 a los ~7 s de terminar la producción y vuelve a 0 a los pocos segundos de terminar el consumo.
