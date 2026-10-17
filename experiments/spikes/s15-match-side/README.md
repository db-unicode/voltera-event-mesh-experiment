# S15. Lado de lectura: matching, transferencia, event mesh y facturación

Comprueba de punta a punta lo que cuelga de la vista Redis, sin el lado de escritura: matching-engine -> matches-db +
outbox -> Kafka -> energy-transfer -> device-sim, y mesh-bridge -> NATS regional -> leafnode -> `BILLING_MATCHED` ->
billing-sim, en r1 y r2.

## Ejecutar

```sh
mise run brokers:kafka && mise run data:up && mise run data:up r2 && mise run mesh:up && mise run spike:s15
```

`spike:s15` ejecuta `apps:match-side all` (build + apply, idempotente). Dura ~9 min: la espera de la ventana de
deduplicación del stream (120 s) y tres sincronizaciones del ConfigMap de reglas pesan casi todo. Variables:
`S15_N` (matches en r1, 20) y `S15_M` (matches en r2, 5). El lado de escritura debe estar parado: el spike cuenta
matches y transferencias por prefijo de medidor, pero comprueba que `energy-transfer` vuelve a 0 réplicas.

## Qué hace

1. Siembra la vista de r1 con `redis-cli` (zona y medidores propios de la corrida, `HSET` + `EXPIRE`) y un precio
   (`price:r1`, versión 1). Hace lo mismo en r2.
2. Espera a que matching-engine cree los matches y comprueba la base y las colas.
3. Sigue cada matchId por device-sim (`byMatchId`) y billing-sim (`/state?detail=1`).
4. Reenvía a Kafka un `EnergyMatched` ya procesado (Job con `kafka-console-producer`, cuerpo tomado de la outbox).

| Check | Verificación |
|---|---|
| (a) | N filas en `matches` (r1), 2N en `matched_positions`, `outbox` con `published_at IS NULL` = 0 |
| (b) | device-sim r1: los N matchId con efecto, cada uno una vez; transfer-db: N `SENT` |
| (c) | billing-sim: los N matchId de r1, una vez cada uno, atribuidos a r1 |
| (d) | Igual con M en r2; `byRegion` separa r1 y r2 |
| (e) | Tras 135 s (fuera de la `duplicate_window` de 120 s del stream) se reenvía un evento: energy-transfer lo registra como duplicado (inbox), device-sim no recibe comando nuevo, billing-sim sube `duplicates` sin cambiar `distinct`. Un comando firmado repetido directo a device-sim responde `duplicate`; con firma inválida, 401 |
| (f) | energy-transfer r1 pasa de 0 réplicas a >=1 y vuelve a 0 (muestras cada 2 s en `s15-replicas-r1.csv`), dos veces |
| Reglas | Un `rules.yaml` que no compila se rechaza y se conserva la versión anterior; una válida nueva cambia la versión; se restaura la original |

Dentro de la ventana de dedupe el stream descarta el repetido por `Nats-Msg-Id` (billing no lo ve); por eso (e) espera.
En ese caso device-sim tampoco recibe nada: la idempotencia de energy-transfer (inbox) corta el duplicado antes.

Evidencia: `experiments/results/s15/<UTC>-<sha>/` (`result.txt`, estados de device-sim y billing, métricas del motor,
logs de los servicios, `top-pods.txt`).
