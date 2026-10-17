# Dashboards de Grafana

- `voltera.json`: "Voltera · EDA, KEDA, CQRS, mesh" (uid `voltera-eda`, refresh 5 s, últimos 15 min, variable `region` = r1/r2/All).
  Filas: KEDA, EDA, CQRS, Idempotencia, Mesh, Zero Trust.
- Aplicar (idempotente): `mise run platform:dashboards`.

## Cómo se provisiona

El addon de Istio no deja montar más ConfigMaps sin editar su Deployment. El task añade la clave `voltera.json`
al ConfigMap `istio-services-grafana-dashboards` (`kubectl patch --type merge`), que ya está montado en
`/var/lib/grafana/dashboards/istio-services` y lo lee el provider de archivo (carpeta `istio`, relectura cada 10 s).
Sobrevive a reinicios del pod: el JSON vive en el ConfigMap, no en el `emptyDir` de Grafana.

- Kubelet tarda hasta ~90 s en refrescar el volumen; el task espera a que el archivo llegue al pod.
- `mise run platform:up` reaplica `grafana.yaml` y no borra la clave (el merge patch no la gestiona `apply`). Si se recrea el ConfigMap, repetir el task.
- NO usar `kubectl apply --server-side` sobre ese ConfigMap: borró las claves del addon.
- Para editar: cambiar `voltera.json` y volver a correr el task. Cambios hechos en la UI no persisten (dashboard provisionado).

## Acceso

```sh
kubectl --context kind-voltera -n istio-system port-forward svc/grafana 3000:3000
# http://localhost:3000/d/voltera-eda
```

## Notas de las queries

- Réplicas = `count by (namespace, app)(up{app=...})` (no hay kube-state-metrics); sin serie = 0 réplicas (KEDA escala a 0).
- Todo se agrega con `sum by` sin `channel` (billing-sim etiqueta channel por matchId).
- Región: `namespace=~"voltera-($region)"`, `vhost=~"$region"`, topic `($region)[.].*`; billing-sim (voltera-global) se incluye siempre en las queries de eventos.
