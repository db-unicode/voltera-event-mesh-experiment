# Datos por servicio (Database per Service)

Cada servicio dueño de datos tiene su propia instancia. Las bases son reutilizables; cada instancia es un
directorio de 10 líneas que fija namespace, prefijo de nombre y etiquetas.

| Base | Imagen | Puerto | Notas |
|---|---|---|---|
| `postgres/` | `postgres:18.6` (multi-arch, verificada con `imagetools`) | `tcp-postgres` 5432 | StatefulSet, PVC 1 Gi (local-path). Montaje en `/var/lib/postgresql` (en PG 18 es el `VOLUME` de la imagen). |
| `redis/` | `redis:8.10.2` (multi-arch) | `tcp-redis` 6379 | Sin persistencia, `maxmemory 64mb`, `volatile-ttl`, `requirepass`. |

Redis 8 se publica con licencias RSALv2/SSPL/AGPLv3 (a elección). Para un experimento académico sin redistribución
no hay problema, y evita cambiar de cliente. Si hiciera falta una licencia permisiva, `valkey/valkey:9.1.2` es
reemplazo directo (misma API; solo cambia la imagen y el comando).

## Instancias

| Región | Namespace | Servicios | Dueño |
|---|---|---|---|
| r1 | `voltera-r1` | `positions-db`, `matches-db`, `transfer-db` (Postgres), `market-view` (Redis) | gateway, matching-engine, energy-transfer, market-projector |
| r2 | `voltera-r2` | `matches-db-r2`, `market-view-r2` | matching-engine y market-projector de r2 |

r2 recibe poca carga y solo necesita las dos instancias que cierran el camino de E8 (matching local y vista); cada
instancia extra cuesta ~100 MiB con su sidecar. Para añadir `positions-db-r2` o `transfer-db-r2`, copiar el
directorio de r1 con `namespace: voltera-r2` y `nameSuffix: -r2` y listarlo en `r2/kustomization.yaml`.

## Credenciales y conexión

Secret generado por kustomize, sin hash en el nombre: `<instancia>-credentials` (Postgres: `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`; Redis: `REDIS_PASSWORD`). Valores de desarrollo: usuario y base `voltera`,
contraseña `voltera-dev`.

```
postgres://voltera:voltera-dev@positions-db.voltera-r1:5432/voltera?sslmode=disable
market-view.voltera-r1:6379   (password voltera-dev)
```

`sslmode=disable`: el cifrado lo pone el mTLS de Istio. Los puertos `tcp-*` hacen que Istio trate el tráfico como TCP.

## Despliegue

```sh
mise run data:up        # r1
mise run data:up r2
```
