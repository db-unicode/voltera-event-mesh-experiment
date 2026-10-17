# S12. Primitivas de caos

Prueba `experiments/lib/chaos.sh` sobre un `go-httpbin` con sidecar en el namespace `s12-chaos` (y un cliente `curl` en `s12-client`).

```sh
mise run spike:s12
```

Imprime `PASS` o `FAIL` por check y guarda la evidencia en `experiments/results/s12/<UTC>-<sha>/` (`result.txt`, `checks/`, `kill_ready.json`). Si todo pasa borra los dos namespaces.

| Check | Primitiva |
|---|---|
| a | httpbin y worker Ready con sidecar (`wait_ready`) |
| b | `chaos_kill`: tiempo kill a Ready medido con `expctl now` y `expctl elapsed` (4 s en kind, un pod) |
| c, d | `chaos_down`, `chaos_up` |
| e, f | `chaos_pause_keda` a 0 y `chaos_resume_keda` sobre un ScaledObject `cron` con `minReplicaCount: 2` |
| g | `chaos_failpoint` por `POST` (ruta `/post` de httpbin) con `expctl http-get --method POST` a un pod con sidecar bajo mTLS STRICT |
| h | `chaos_failpoint` con endpoint inexistente: cae a `kubectl set env FAILPOINTS=` |
| i-l | `chaos_partition` (DENY por namespace de origen y puerto de destino): 200 a 403 y de vuelta a 200 con `chaos_heal` |

El DENY de Istio sobre HTTP aplica por petición y tarda unos 3 s en propagarse. Sobre TCP solo afecta conexiones nuevas (ver S8).
