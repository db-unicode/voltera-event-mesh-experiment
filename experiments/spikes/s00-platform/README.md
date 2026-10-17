# S0. Plataforma base

Cluster kind con Istio, KEDA, observabilidad y metrics-server. Base de todos los demás spikes.

## Ejecutar

```sh
mise run cluster:up && mise run platform:up && mise run spike:s00
```

Los tres pasos son idempotentes. `spike:s00` imprime `PASS` o `FAIL` (exit 0/1) y guarda la evidencia en `experiments/results/s00/<UTC>-<sha>/` (`-dirty` si hay cambios sin commitear).

## Qué instala `platform:up`

| Pieza | Versión | Namespace | Origen |
|---|---|---|---|
| Istio (perfil `minimal`, solo istiod) | 1.31.2 | `istio-system` | `deploy/platform/istio/istio-operator.yaml` |
| PeerAuthentication STRICT (toda la malla) y Telemetry (100 % de trazas) | n/a | `istio-system` | `deploy/platform/istio/` |
| KEDA | chart 2.21.0 = v2.21.0 | `keda` (sin sidecar) | Helm, repo `kedacore` |
| Prometheus 3.10.0, Kiali 2.31.0, Grafana 12.0.1, Jaeger 2.17.0 | addons de Istio 1.31.2 | `istio-system` | `deploy/platform/observability/` (YAML versionado, sin cambios) |
| metrics-server | v0.9.0 + `--kubelet-insecure-tls` | `kube-system` | `deploy/platform/metrics-server/` |

Los addons de Istio quedan en `istio-system` porque sus YAML y la config de Kiali lo asumen. Son más viejos que las versiones del informe 02 (Prometheus 3.15, Kiali 2.33, Grafana 13.2); se prefirió la combinación probada por Istio y no depender de red al aplicar.

## Qué comprueba

1. Nodo Ready, istiod y KEDA operator disponibles, CRD `scaledobjects.keda.sh`.
2. Prometheus, Kiali, Grafana y Jaeger disponibles y respondiendo por el proxy del API server (sin port-forward). Kiali reporta Prometheus entre sus servicios externos.
3. `httpbin` con sidecar en `s00-mesh` (`istio-injection=enabled`): app e `istio-proxy` listos.
4. Llamada desde un cliente con sidecar: funciona y `X-Forwarded-Client-Cert` trae la identidad SPIFFE del cliente (prueba de mTLS).
5. Cliente sin sidecar en `s00-plain`: llega a Prometheus (control positivo) y es rechazado al llamar a `httpbin` (connection reset, curl 56).
6. Prometheus registra `istio_requests_total` de `httpbin`.
7. `kubectl top nodes` y `kubectl top pods -A`, más RAM por namespace (`ram-by-namespace-mib.tsv`) y `docker stats` del nodo.

Al pasar, el spike borra los namespaces `s00-*`. Si falla, los deja para depurar.

## Problemas encontrados

- **El perfil `minimal` no emite `istio_requests_total`.** Sin `meshConfig.defaultProviders.metrics: [prometheus]` el sidecar no instala el filtro de estadísticas y Kiali/Prometheus no ven tráfico. Solución en `istio-operator.yaml`.
- **El sidecar es un init container nativo.** Con Istio 1.31 y Kubernetes 1.37, `istio-proxy` aparece en `initContainers` (con `restartPolicy: Always`) y no en `containers`. `kubectl get pod` muestra `2/2`, pero `.spec.containers` tiene 1 elemento. El check cuenta contenedores y init containers restartables.
- **`kind create cluster` cambia el current-context** de `~/.kube/config` a `kind-voltera`. Todo el repo usa `--context kind-voltera` explícito, así que no depende de ello.
- **Imágenes:** todas tienen manifiesto `linux/arm64` (verificado con `docker buildx imagetools inspect`): `kindest/node`, `ghcr.io/kedacore/keda`, `mccutchen/go-httpbin:2.26.0`, `curlimages/curl:8.23.0`, metrics-server y los addons. No hubo emulación.
- **kubeconform** no tiene esquemas para CRDs de Istio, KEDA ni kind; se ignoran con `-ignore-missing-schemas`. Solo se validan los recursos core.

## Evidencia

- `result.txt`: un PASS/FAIL por check y veredicto final. `checks/<n>.log`: salida de cada check.
- `top-nodes.txt`, `top-pods.txt`, `ram-by-namespace-mib.tsv`, `docker-stats-node.txt`, `pods.txt`, `mtls-headers.json`.

## RAM medida (plataforma sin brokers ni servicios, 60 s tras `platform:up`)

Una corrida en macOS arm64, Docker (OrbStack) con 15,6 GiB. Suma de `kubectl top pods -A`:

| Namespace | MiB |
|---|---|
| `kube-system` (apiserver 468, etcd 55, controller-manager 74, resto) | 716 |
| `istio-system` (istiod 60, Prometheus 170, Grafana 130, Kiali 39, Jaeger y otros) | 419 |
| `keda` | 91 |
| `local-path-storage` | 14 |
| **Total pods** | **1240** |

`docker stats` del contenedor del nodo: 1,55 GiB (incluye kubelet, containerd y kernel). El estimado del informe 02 para esta parte era ~2,4 GiB (nodo 0,9 + istiod 0,3 + Kiali 0,15 + Prometheus 0,4 + Grafana 0,2 + Jaeger 0,25 + KEDA 0,25); lo medido es ~35 % menor. Sin carga: Prometheus y Jaeger crecerán con las series y las trazas.
