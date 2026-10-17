# S3. KEDA con trigger Kafka: 0 -> N -> 0

`energy-transfer-probe` (imagen `kafka-probe`, subcomando `work`, 50 ms por evento) consume `r1.energy-matched` con el grupo `energy-transfer-probe`. Un ScaledObject con trigger `kafka` lo escala según el lag.

## Ejecutar

```sh
mise run brokers:kafka && mise run platform:up && mise run spike:s03
```

`platform:up` es necesario una vez: aplica el acceso de KEDA al mesh (ver abajo). En un cluster que ya tenía KEDA instalado, además hay que reiniciar el operador (`kubectl -n keda rollout restart deploy/keda-operator`) para que reciba el sidecar.

## Configuración (`scaledobject.yaml`)

`minReplicaCount: 0`, `maxReplicaCount: 6` (= particiones), `pollingInterval: 5`, `cooldownPeriod: 30`, HPA `scaleDown.stabilizationWindowSeconds: 30`. Trigger: `bootstrapServers: kafka.voltera-infra.svc:9092`, `lagThreshold: "50"`, `offsetResetPolicy: earliest`, `limitToPartitionsWithLag: "true"`.

## Flujo

1. Deployment en 0 réplicas, sin ScaledObject; `seek --to end` para el grupo (sin backlog de otras corridas; con `earliest` y sin offsets KEDA vería todo el log como lag). Después se aplica el ScaledObject y se espera `Ready`.
2. 12 s sin eventos: se queda en 0.
3. Un muestreador escribe `ts,replicas,lag` cada ~2 s en `s03-samples.csv` (réplicas de `.status.replicas`; lag leído del exporter en cada muestra, sin esperar el scrape). El Job produce 3000 eventos a 300/s (~10 s).
4. Pasa si el CSV arranca en 0, el pico está entre 2 y 6, termina en 0 réplicas con lag 0, y Prometheus también ve lag 0.

Evidencia: `experiments/results/s03/<UTC>-<sha>/` con `s03-samples.csv`, `scaledobject.json`, `scaledobject-hpa.yaml`, `scaledobject-describe.txt`, `keda-operator.log` y `prom-events-consumed.json` (suma de `events_consumed_total` de todos los pods: 3000).

## Cómo llega KEDA a Kafka (decisión Zero Trust)

Problema: el operador de KEDA corre en `keda` sin sidecar y el broker exige mTLS STRICT. Sin cambios, el ScaledObject queda `Ready=False` con `KEDAScalerFailed: error creating kafka client: ... read: connection reset by peer`.

Opciones:

| Opción | Resultado |
|---|---|
| PeerAuthentication PERMISSIVE en el puerto 9092 de Kafka | Descartada: cualquier pod del cluster, con o sin identidad, podría hablar en claro con el broker. Un `AuthorizationPolicy` no distingue a los clientes en claro (sin principal) |
| Sidecar en todo KEDA | Descartada: el API server (fuera del mesh) llama al adaptador de métricas y a los webhooks, así que habría que abrir esos puertos igualmente |
| **Sidecar solo en `keda-operator` (elegida)** | El operador tiene identidad SPIFFE `cluster.local/ns/keda/sa/keda-operator` y habla mTLS con Kafka; el broker sigue en STRICT |

Con el sidecar en el operador, el adaptador de métricas (sin sidecar) deja de poder llamarle por gRPC (puerto 9666, TLS propio de KEDA): `grpc: addrConn.createTransport failed to connect ... transport: authentication handshake failed`. Se comprobó borrando el recurso y reiniciando el adaptador. Por eso hay un `PeerAuthentication` con PERMISSIVE solo en el 9666 del operador; el resto de sus puertos sigue en STRICT. Es la excepción residual: ese puerto lo protege el TLS interno de KEDA, no el mesh.

Cambios hechos fuera de este spike (plataforma):

- `deploy/platform/keda/values.yaml`: `podLabels.keda: sidecar.istio.io/inject: "true"` y se quita la anotación `inject: "false"` del operador. Adaptador y webhooks siguen sin sidecar.
- `deploy/platform/keda/peer-authentication.yaml` (nuevo): PERMISSIVE en 9666 para `app=keda-operator`.
- `mise-tasks/platform/up`: crea el namespace `keda` antes de Helm, le quita la etiqueta `istio-injection` (con `disabled` el webhook ignora la etiqueta del pod) y aplica el PeerAuthentication.
- En el cluster vivo: `mise run platform:up` y `rollout restart` del operador. No se tocó Istio ni el resto de KEDA.

Pendiente (no hecho): un `AuthorizationPolicy` en Kafka que permita solo los principales de los clientes. Activarla pasa a denegar por defecto a todo lo demás, así que conviene hacerlo cuando estén todos los servicios.

Efecto en S5: el operador ya puede llegar a RabbitMQ dentro del mesh con mTLS, sin más cambios de plataforma.
