# 0002. Cluster local con kind

- Estado: aceptada
- Fecha: 2026-10-16

## Contexto y problema

El experimento debe correr igual en Linux y macOS, sin depender de OrbStack Kubernetes (el repo anterior usaba el cluster de OrbStack y el overlay `kind` era una copia sin uso).

## Opciones consideradas

| Opción | A favor | En contra |
|---|---|---|
| kind v0.33.0 | Kubernetes sin recortes; plataforma de referencia en los docs de Istio; mismo comportamiento en Linux y macOS | ~0,8-1,0 GiB de RAM idle por nodo (est.) |
| k3d v5.9.0 | ~0,5-0,7 GiB por nodo (est.); registro local integrado | k3s recortado; hay que desactivar Traefik y ServiceLB para Istio |
| OrbStack Kubernetes | Cero configuración en macOS | Solo macOS; no reproducible en Linux |

## Decisión

kind con un nodo control-plane, imagen `kindest/node:v1.37.0` fijada por digest y nombre de cluster `voltera`. Se accede a las UIs con `kubectl port-forward`, sin `extraPortMappings`.

## Consecuencias

- Istio 1.31.2 soporta Kubernetes 1.32-1.37, así que cubre el nodo v1.37.0.
- Hay que fijar `kindest/node` por digest; sin él kind avisa del cambio de imagen.
- Linux: `fs.inotify.*` bajos producen "too many open files"; `mise run doctor` lo verifica.
- k3d queda como alternativa si la RAM aprieta (ahorro est. 300-400 MiB).
- Las imágenes deben ser multi-arch; una imagen solo amd64 corre emulada en macOS arm64.
