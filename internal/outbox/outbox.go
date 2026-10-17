// Package outbox implementa el patrón outbox transaccional sobre Postgres: Append
// inserta el evento dentro de la transacción de negocio y Relay lo publica después
// con entrega al-menos-una-vez a través de un Publisher.
package outbox

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations es la fuente de migraciones de la tabla outbox (migrate.Up).
func Migrations() migrate.Source {
	return migrate.Source{Name: "outbox", FS: migrationsFS, Dir: "migrations"}
}

// DBTX lo cumplen pgx.Tx, *pgxpool.Pool y *pgx.Conn.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Append inserta el evento en la outbox. Debe llamarse DENTRO de la transacción de
// negocio (db = pgx.Tx): estado y evento se confirman o se revierten juntos.
// topic y key son el destino y la clave de partición/ruta que usará el Publisher.
// Un eventId repetido devuelve error (índice único).
func Append(ctx context.Context, db DBTX, env events.Envelope, topic, key string) error {
	payload, err := events.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	if topic == "" {
		return fmt.Errorf("outbox: empty topic for event %s", env.EventID)
	}
	_, err = db.Exec(ctx,
		`INSERT INTO outbox (event_id, aggregate_id, type, topic, key, payload) VALUES ($1, $2, $3, $4, $5, $6)`,
		env.EventID, env.AggregateID, env.Type, topic, key, payload)
	if err != nil {
		return fmt.Errorf("outbox: append %s: %w", env.EventID, err)
	}
	return nil
}
