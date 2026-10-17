// Package adapters contiene el adaptador Postgres del declaration-gateway.
package adapters

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
)

// OutboxTopic es el canal (cola energy-positions) al que el relay publica PositionDeclared.
const OutboxTopic = "energy-positions"

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations es la fuente de migraciones de la tabla positions (migrate.Up).
func Migrations() migrate.Source {
	return migrate.Source{Name: "positions", FS: migrationsFS, Dir: "migrations"}
}

// upsertSQL aplica la posición solo si su versión es mayor que la guardada: 0 filas
// afectadas = duplicado o mensaje viejo.
const upsertSQL = `
INSERT INTO positions (meter_id, side, kwh, valid_until, zone, version, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())
ON CONFLICT (meter_id) DO UPDATE
   SET side = EXCLUDED.side, kwh = EXCLUDED.kwh, valid_until = EXCLUDED.valid_until,
       zone = EXCLUDED.zone, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at
 WHERE positions.version < EXCLUDED.version`

// PositionStore guarda las posiciones en positions-db y el evento en la outbox.
type PositionStore struct {
	pool *pgxpool.Pool
}

// NewPositionStore crea el store sobre pool (la base del gateway, con las migraciones aplicadas).
func NewPositionStore(pool *pgxpool.Pool) *PositionStore { return &PositionStore{pool: pool} }

// Apply guarda la posición y, en la MISMA transacción, el evento env en la outbox. Si la
// versión no es mayor que la guardada, no escribe nada y devuelve applied=false.
func (s *PositionStore) Apply(ctx context.Context, env events.Envelope, p events.PositionDeclared) (bool, error) {
	applied := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, upsertSQL, p.MeterID, string(p.Side), p.KWh, p.ValidUntil, p.Zone, env.Version)
		if err != nil {
			return fmt.Errorf("upsert position %s: %w", p.MeterID, err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		if err := outbox.Append(ctx, tx, env, OutboxTopic, p.MeterID); err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// Count devuelve cuántas posiciones hay (para /debug/state).
func (s *PositionStore) Count(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM positions`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count positions: %w", err)
	}
	return n, nil
}
