package matching

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations es la fuente de migraciones de matches-db (junto con outbox.Migrations()).
func Migrations() migrate.Source {
	return migrate.Source{Name: "matching", FS: migrationsFS, Dir: "migrations"}
}

// Store persiste los matches en matches-db.
type Store struct {
	pool   *pgxpool.Pool
	region string
	topic  string
	now    func() time.Time
	// Mode es ModeOutbox salvo en el control de E6 (ModeDirect: sin fila de outbox).
	Mode OutboxMode
}

// NewStore crea el Store; los eventos se encolan en la outbox con topic <region>.energy-matched.
func NewStore(pool *pgxpool.Pool, region string) *Store {
	return &Store{pool: pool, region: region, topic: region + ".energy-matched", now: time.Now, Mode: ModeOutbox}
}

// Topic es el topic de Kafka de los EnergyMatched de la región.
func (s *Store) Topic() string { return s.topic }

// MatchedRefs devuelve de las posiciones dadas las (meterId, version) que ya están en
// matched_positions: el motor las descarta antes de planificar para no volver a
// proponerlas ventana tras ventana mientras la posición siga viva en la vista.
func (s *Store) MatchedRefs(ctx context.Context, positions []Position) (map[string]struct{}, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	ids := make([]string, len(positions))
	vers := make([]int64, len(positions))
	for i, p := range positions {
		ids[i], vers[i] = p.MeterID, p.Version
	}
	rows, err := s.pool.Query(ctx, `
		SELECT mp.meter_id, mp.version FROM matched_positions mp
		JOIN unnest($1::text[], $2::bigint[]) AS q(meter_id, version) USING (meter_id, version)`, ids, vers)
	if err != nil {
		return nil, fmt.Errorf("matching: matched refs: %w", err)
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var id string
		var v int64
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[Position{MeterID: id, Version: v}.Ref()] = struct{}{}
	}
	return out, rows.Err()
}

// Created es un match confirmado.
type Created struct {
	Event   events.Envelope
	Payload events.EnergyMatched
}

// Persist guarda los pares de una zona y ventana en UNA transacción. Cada par entra
// en un savepoint: inserta el match, marca (meterId, version) de ambas posiciones en
// matched_positions (PK) y encola el EnergyMatched en la outbox. Si alguna posición ya
// estaba emparejada (otra ventana o réplica) o el matchId ya existe, se deshace solo
// ese par y se cuenta como descartado. Devuelve los matches creados (ya confirmados).
func (s *Store) Persist(ctx context.Context, window, zone string, price Price, pairs []Pair) (created []Created, discarded int, err error) {
	if len(pairs) == 0 {
		return nil, 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("matching: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	for _, p := range pairs {
		c, ok, err := s.persistPair(ctx, tx, window, zone, price, p)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			discarded++
			continue
		}
		created = append(created, c)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, fmt.Errorf("matching: commit: %w", err)
	}
	return created, discarded, nil
}

var errPairTaken = errors.New("position already matched")

func (s *Store) persistPair(ctx context.Context, tx pgx.Tx, window, zone string, price Price, p Pair) (Created, bool, error) {
	matchID := events.MatchID(window, p.Seller.Ref(), p.Buyer.Ref())
	payload := events.EnergyMatched{MatchID: matchID, SellerMeterID: p.Seller.MeterID, BuyerMeterID: p.Buyer.MeterID,
		KWh: p.KWh, PricePerKWh: price.PricePerKWh, Zone: zone, Window: window}
	env, err := events.NewEnergyMatched(s.region, 1, s.now(), payload)
	if err != nil {
		return Created{}, false, fmt.Errorf("matching: build event: %w", err)
	}
	env = env.WithTraceparent(NewTraceparent())

	sp, err := tx.Begin(ctx) // savepoint
	if err != nil {
		return Created{}, false, fmt.Errorf("matching: savepoint: %w", err)
	}
	err = func() error {
		tag, err := sp.Exec(ctx, `
			INSERT INTO matches (match_id, seller_meter_id, buyer_meter_id, kwh, price_per_kwh, zone, "window")
			VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`,
			matchID, payload.SellerMeterID, payload.BuyerMeterID, payload.KWh, payload.PricePerKWh, zone, window)
		if err != nil {
			return fmt.Errorf("matching: insert match: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return errPairTaken
		}
		for _, pos := range []Position{p.Seller, p.Buyer} {
			tag, err := sp.Exec(ctx,
				`INSERT INTO matched_positions (meter_id, version, match_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				pos.MeterID, pos.Version, matchID)
			if err != nil {
				return fmt.Errorf("matching: mark position %s: %w", pos.Ref(), err)
			}
			if tag.RowsAffected() == 0 {
				return errPairTaken
			}
		}
		if s.Mode == ModeDirect {
			return nil // control de E6: el evento no se registra en la misma transacción
		}
		return outbox.Append(ctx, sp, env, s.topic, matchID)
	}()
	if err == nil {
		if err := sp.Commit(ctx); err != nil {
			return Created{}, false, fmt.Errorf("matching: release savepoint: %w", err)
		}
		return Created{Event: env, Payload: payload}, true, nil
	}
	_ = sp.Rollback(context.WithoutCancel(ctx))
	if errors.Is(err, errPairTaken) {
		return Created{}, false, nil
	}
	return Created{}, false, err
}
