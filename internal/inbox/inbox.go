// Package inbox da idempotencia de consumidor sobre Postgres: TryMark inserta la
// marca (consumidor, evento) dentro de la misma transacción que aplica el efecto,
// de modo que marca y efecto se confirman o se revierten juntos.
package inbox

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations es la fuente de migraciones de la tabla inbox (migrate.Up).
func Migrations() migrate.Source {
	return migrate.Source{Name: "inbox", FS: migrationsFS, Dir: "migrations"}
}

// DBTX lo cumplen pgx.Tx, *pgxpool.Pool y *pgx.Conn.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// TryMark intenta registrar eventKey para consumer. fresh=true si es la primera vez
// (el llamador debe aplicar el efecto); false si ya estaba (duplicado: descartar).
// Úsalo con la transacción de negocio: si esta se revierte, la marca también.
// eventKey suele ser el eventId, o el matchId cuando la idempotencia es de negocio.
func TryMark(ctx context.Context, db DBTX, consumer, eventKey string) (fresh bool, err error) {
	tag, err := db.Exec(ctx,
		`INSERT INTO inbox (consumer, event_key) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumer, eventKey)
	if err != nil {
		return false, fmt.Errorf("inbox: mark %s/%s: %w", consumer, eventKey, err)
	}
	return tag.RowsAffected() == 1, nil
}
