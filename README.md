# Voltera event mesh experiment

Experimento universitario de arquitectura orientada a eventos para Voltera, un motor de emparejamiento P2P de energía. Mide con spikes y experimentos reproducibles:

- Cola durable (RabbitMQ) frente a topic con log retenido (Kafka).
- Event mesh entre dos regiones (NATS).
- Autoescalado por cola y por lag (KEDA).
- CQRS con outbox e idempotencia.
- Zero Trust con Istio (mTLS STRICT).

Todo corre en un cluster kind de un nodo, igual en Linux y macOS (amd64 y arm64).

## Requisitos

- Docker en ejecución, con al menos 10 GiB de RAM asignados (12 GiB recomendados en macOS).
- [mise](https://mise.jdx.dev). Instala y fija go, kind, kubectl, helm, istioctl, golangci-lint, shellcheck, kubeconform, jq y natscli.
- Linux: `fs.inotify.max_user_instances >= 512` y `max_user_watches >= 524288` (`mise run doctor` avisa).

## Quickstart

```sh
mise install && mise run doctor && mise run cluster:up && mise run platform:up && mise run spike:s00
```

El spike imprime `PASS` o `FAIL` y deja la evidencia en `experiments/results/s00/<UTC>-<sha>/`.

## Tasks

| Task | Qué hace |
|---|---|
| `doctor` | Verifica herramientas, Docker, RAM de Docker e inotify |
| `lint` | `go vet`, golangci-lint, shellcheck y kubeconform sobre `deploy/` |
| `test` | `go test -race ./...` |

## Estructura

| Ruta | Contenido |
|---|---|
| `cmd/`, `internal/` | Servicios Go (aún vacío) |
| `deploy/{kind,platform,brokers,mesh,apps}/` | Manifiestos del cluster, plataforma, brokers, mesh y aplicaciones |
| `experiments/{spikes,lib,results}/` | Spikes `sNN`, librería shell y evidencia (solo `results/final/` se versiona) |
| `mise-tasks/` | Tasks de mise como archivos (`cluster/up` es `cluster:up`) |
| `docs/` | Documentación y ADRs (`docs/adr/`) |

## Convenciones

- Scripts POSIX `sh` (`set -eu`), validados con `shellcheck -s sh`.
- Código e identificadores en inglés; documentación en español.
- Commits `type(scope): <gitmoji> summary`; trunk-based con ramas cortas y merge `--no-ff`.
