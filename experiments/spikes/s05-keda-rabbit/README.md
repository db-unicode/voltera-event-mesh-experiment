# S5. KEDA con RabbitMQ: 0 -> N -> 0 y capacidad por pod

Mide `C_pod` (msgs/s sostenidos de una réplica) y valida que el scaler `rabbitmq` de KEDA escala `declaration-gateway-probe` de 0 a N y de vuelta a 0 sobre una quorum queue.

## Ejecutar

```sh
mise run brokers:rabbitmq && mise run images:build amqp-probe
mise run spike:s05                 # KEDA_VALUE=234 mise run spike:s05 para probar otro value
```

Evidencia en `experiments/results/s05/<UTC>-<sha>/`: `result.txt`, `c_pod.txt`, `burst.txt`, `samples.csv` (`ts,replicas,depth` cada 2 s), `prom-rate.json`, `scaledobject.yaml`, `hpa-describe.txt`, `events.txt`, `k8s-state.txt`. Al pasar borra `s05-keda-rabbit`.

## Montaje

- `manifests.yaml`: namespace (con sidecar), Deployment `declaration-gateway-probe` (`amqp-probe work --vhost=r1 --queue=meter-declarations --prefetch=10 --delay=20ms`, usuario `declaration-gateway-r1`) y `TriggerAuthentication` `rabbitmq-http`.
- `scaledobject.yaml`: trigger `rabbitmq` con `protocol: http`, `mode: QueueLength`, `value: "100"`, min 0, max 10, `pollingInterval: 5`, `cooldownPeriod: 30`, HPA `scaleDown.stabilizationWindowSeconds: 30`.
- `run.sh` crea el Secret `keda-rabbitmq-uri` (`http://keda:<contraseña>@rabbitmq.voltera-infra.svc:15672/r1`) a partir del Secret de `voltera-infra`; la contraseña no está en ningún manifiesto. El usuario `keda` tiene el tag `monitoring`: ve las colas (`GET /api/queues/r1/meter-declarations`) pero no los usuarios (401) ni puede publicar ni consumir.

## Cómo llega KEDA al management API (15672)

El mesh exige mTLS STRICT. El operador de KEDA corre ahora con sidecar Istio (cambio del spike de Kafka en la plataforma: identidad `cluster.local/ns/keda/sa/keda-operator`), así que su petición HTTP sale del sidecar con mTLS. En este spike no se cambió la plataforma ni se relajó mTLS en RabbitMQ. Capas en `deploy/brokers/rabbitmq/`:

1. `security.yaml`, `AuthorizationPolicy` ALLOW: cualquier identidad del mesh en los puertos distintos de 15672; en el 15672 solo `cluster.local/ns/keda/sa/keda-operator`.
2. `network-policy.yaml`, `NetworkPolicy`: el 15672 solo admite pods `app.kubernetes.io/name=keda-operator` del namespace `keda`. kindnet la aplica (comprobado con un curl entre namespaces).
3. Credenciales: usuario `keda` con permisos vacíos y tag `monitoring`.

Comprobaciones: `spike:s05` verifica que un pod de otro namespace no llega al 15672 (la NetworkPolicy lo corta, HTTP 000). A mano, un pod en `keda` con la etiqueta del operador pero otra identidad pasa la NetworkPolicy y recibe `403 RBAC: access denied` de la AuthorizationPolicy.

## Resultados

- **`C_pod` = 46,3-46,7 msgs/s** (4 corridas: 46,7 / 46,2 / 46,3 / 46,3). Contador `events_consumed_total` del pod durante 30 s con la cola llena (4000 mensajes), tras 10 s de calentamiento; `rate(events_consumed_total[30s])` en Prometheus da 46,3-46,5. Es el techo teórico de un worker secuencial con `--delay=20ms` (50/s) menos el ida y vuelta del ack.
- Ráfaga: `10 x C_pod` = 463-467 mensajes a ~`C_pod` msgs/s (~10 s) con `amqp-probe publish --count --rate`.
- Serie con `value: 100` (tiempos desde la primera muestra de `samples.csv`; la ráfaga empieza ~8 s después): 0 réplicas, 1 réplica a los 14-19 s, 2-3 réplicas a los ~29 s (pico de profundidad muestreado 345-437), cola en 0 a los ~34 s, 0 réplicas a los ~58 s. El pico de réplicas fue 3, 2 y 4 en las tres corridas con `value: 100`.
- Con `value: 234` (= `C_pod x 5`) el pico fue 2 réplicas: pasa el criterio sin margen.
- Resultado: 0 -> >= 2 -> 0 y profundidad final 0. `spike:s05` terminó en PASS con `value: 100` en la corrida final; las corridas con `value: 100` anteriores pasaron los checks de escalado, y la de `value: 234` también.

Recomendación para `params.env`: `C_POD=46` y, si se quiere el `value = C_pod x 5` del informe, `KEDA_VALUE=230`; con ese valor y una ráfaga de `10 x C_pod` el margen sobre 2 réplicas es mínimo. Con `value: 100` hay margen.

## Hallazgos

- **La profundidad que ve KEDA tiene hasta 5 s de retraso.** La API de management refresca las estadísticas de cola cada `collect_statistics_interval` (5 s). El muestreo en `samples.csv` (misma API) muestra escalones de 4-6 s y el pico real es mayor que el muestreado.
- **La baja a 0 la hace KEDA, no el HPA.** El HPA tiene mínimo 1; al cumplirse `cooldownPeriod` (30 s tras el último trigger activo) KEDA pone el Deployment en 0 de golpe (`from 2 to 0` / `from 3 to 0`). La ventana de estabilización de 30 s del HPA no se observa en esta curva.
- **`protocol: http` cuenta ready + unacked** (`messages` de la API); funciona con quorum queues (el pendiente 5 del informe 02).
- **Latencia de arranque desde 0:** la primera réplica (activación de KEDA, poll de 5 s) y las siguientes (sync del HPA, 15 s) están separadas por ~10-15 s en las series medidas (`primera_replica_t` y `dos_replicas_t` en el log del check de análisis).
- Si el pod se mata durante `process` (scale-in), no se hace ack y el broker reentrega el mensaje a otra réplica.
