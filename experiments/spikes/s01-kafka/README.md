# S1. Kafka con sidecar Istio

Kafka 4.3.1 (KRaft, 1 nodo con roles `broker,controller`) dentro del mesh. Produce, consume y replay desde Jobs con sidecar. Incluye la pieza Kafka definitiva (`deploy/brokers/kafka/`) y el cliente de prueba `kafka-probe`.

## Ejecutar

```sh
mise run brokers:kafka && mise run spike:s01
```

`brokers:kafka` es idempotente: aplica Kafka y kafka-exporter, espera el rollout, recrea el Job de topics (los Jobs son inmutables) y muestra `--describe`. `spike:s01` imprime `PASS` o `FAIL`, guarda la evidencia en `experiments/results/s01/<UTC>-<sha>/` y borra `s01-kafka` si pasa.

## Qué se despliega (`voltera-infra`)

| Recurso | Detalle |
|---|---|
| StatefulSet `kafka` | `apache/kafka:4.3.1` (amd64+arm64), 1 réplica, heap `-Xms256m -Xmx512m`, requests 200m / 768Mi, límite 1Gi, PVC 2Gi (`standard`, local-path) |
| Service `kafka-headless` | DNS estable del pod; el broker anuncia `kafka-0.kafka-headless.voltera-infra.svc.cluster.local:9092` |
| Service `kafka` | Bootstrap de los clientes: `kafka.voltera-infra.svc:9092`, puerto `tcp-kafka` |
| Job `kafka-topics` | Crea `r1.energy-matched` y `r2.energy-matched`: 6 particiones, replicación 1, `retention.ms=604800000`, `min.insync.replicas=1`. `--create --if-not-exists` más `kafka-configs --alter` para reconciliar la config |
| Deployment `kafka-exporter` y Service | `danielqsj/kafka-exporter:v1.10.0` (amd64+arm64), anotado para Prometheus |

Decisiones:

- **Listeners.** `PLAINTEXT://:9092` (clientes y entre brokers) y `CONTROLLER://localhost:9093`: el quorum solo escucha en loopback, no sale del pod. La seguridad del puerto 9092 la da el mesh (mTLS STRICT), no Kafka.
- **Topics sin auto-creación** (`auto.create.topics.enable=false`); los crea el Job.
- **Job de topics con sidecar.** Con mTLS STRICT un cliente sin sidecar es rechazado, así que el Job lleva sidecar. En Istio 1.31 + Kubernetes 1.37 `istio-proxy` es un init container nativo (`restartPolicy: Always`), por lo que el Job termina solo cuando sale el contenedor principal (comprobado: pod `Completed`, Job `Complete`). No hace falta `curl -X POST .../quitquitquit`.
- **Probes por `exec nc -z localhost 9092`.** Un probe TCP del kubelet contra 9092 lo acepta Envoy aunque Kafka siga arrancando. Con `holdApplicationUntilProxyStarts` (global) el contenedor `kafka` se crea cuando el proxy ya está listo; los eventos del pod lo muestran (startup probe de 15021 falla hasta que el proxy arranca, después se hace el pull y se crea `kafka`).
- **kafka-exporter reinicia 2-3 veces al crear** el Deployment junto con el broker (`Error Init Kafka Client`) hasta que Kafka acepta conexiones. Es esperado y se estabiliza solo.
- `kubectl apply -k` muestra `statefulset.apps/kafka configured` en cada ejecución sin cambiar `generation`: es el ruido conocido de `volumeClaimTemplates`.

## kafka-probe

`cmd/kafka-probe` (lógica en `internal/kafkaprobe`, franz-go v1.22.2). Config: `KAFKA_BROKERS` (por defecto `kafka.voltera-infra.svc:9092`) y `METRICS_ADDR` (`:8080`).

| Subcomando | Qué hace |
|---|---|
| `produce --topic --count --key-prefix [--rate]` | acks=all, productor idempotente; claves `<prefix>-<n>`; `--rate` limita eventos/s |
| `consume --topic --group --expect N --timeout [--key-prefix]` | commit manual tras cada lote; imprime `count=N distinct=M`; falla si no llega a N. `--key-prefix` ignora eventos ajenos (el topic es compartido entre corridas) |
| `work --topic --group --delay 50ms` | consumidor continuo y lento; `/metrics` (`events_consumed_total{topic,group}`) y `/healthz` en `METRICS_ADDR` |
| `seek --topic --group --to end\|start` | confirma el final o inicio del log para un grupo (crea el grupo; sin miembros activos) |

## Qué comprueba

1. Kafka y kafka-exporter listos; topics con la config pedida.
2. Job con sidecar produce 1000 eventos; el Job termina solo (pod `Succeeded`, `istio-proxy` como init container con `restartPolicy: Always`).
3. Grupo A nuevo consume 1000/1000. Grupo B nuevo, desde `earliest`, consume otros 1000/1000: el log sigue ahí tras el primer consumo.
4. Prometheus: `istio_tcp_connections_opened_total` hacia `kafka` con mTLS.

Las claves llevan un prefijo único por corrida (`s01-<UTC>`), así que repetir el spike sobre el mismo topic da siempre 1000.

## Métrica del mesh (etiquetas exactas)

```
istio_tcp_connections_opened_total{
  reporter="destination",
  connection_security_policy="mutual_tls",
  destination_workload="kafka", destination_workload_namespace="voltera-infra",
  destination_service_name="kafka-headless",
  source_workload="s01-produce", source_workload_namespace="s01-kafka" }
```

- `connection_security_policy="mutual_tls"` solo aparece con `reporter="destination"`. Con `reporter="source"` vale `unknown`.
- `destination_service_name` es `kafka-headless` para las conexiones de datos (el broker anuncia el DNS del pod) y `kafka` solo para el bootstrap.
- `source_workload` es el nombre del Job/Deployment del cliente: la identidad llega al broker.

## Problemas encontrados

- Valores YAML con coma dentro de un mapa en línea (`value: broker,controller`) se parten en dos claves; hay que entrecomillarlos.
- `--kafka.version=2.8.0` en el exporter (el valor por defecto es `2.0.0`); no se probó la diferencia, funciona con 2.8.0.
- Grupos y offsets: ver README de S2 y S3 (`seek` evita arrastrar backlog de otras corridas).

## RAM medida (`kubectl top pods`, tras correr S1-S3)

| Pod | MiB |
|---|---|
| `kafka-0` (kafka 364 + istio-proxy 39) | ~404 |
| `kafka-exporter` (11 + istio-proxy 41) | ~53 |

El estimado del informe 02 para Kafka era 0,7-0,9 GiB; con heap acotado y poco tráfico queda en ~0,4 GiB.
