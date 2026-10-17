package adapters

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

func decl(t *testing.T, meter string, version int64, kwh float64) (events.Envelope, events.PositionDeclared) {
	t.Helper()
	p := events.PositionDeclared{MeterID: meter, Side: events.SideSurplus, KWh: kwh, ValidUntil: time.Now().Add(time.Minute).UTC(), Zone: "z-1"}
	env, err := events.NewPositionDeclared("r1", version, time.Now(), p)
	require.NoError(t, err)
	return env, p
}

func TestApplyKeepsOnlyNewerVersionsAndWritesOutboxAtomically(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	require.NoError(t, migrate.Up(ctx, pool, outbox.Migrations(), Migrations()))
	s := NewPositionStore(pool)

	count := func(q string, args ...any) int {
		var n int
		require.NoError(t, pool.QueryRow(ctx, q, args...).Scan(&n))
		return n
	}

	env1, p1 := decl(t, "m-1", 5, 4.2)
	ok, err := s.Apply(ctx, env1, p1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, count(`SELECT count(*) FROM outbox WHERE topic = 'energy-positions' AND key = 'm-1'`))

	// misma versión (duplicado) y versión menor (viejo): sin cambios ni outbox
	envDup, pDup := decl(t, "m-1", 5, 9.9)
	ok, err = s.Apply(ctx, envDup, pDup)
	require.NoError(t, err)
	require.False(t, ok)
	envOld, pOld := decl(t, "m-1", 3, 9.9)
	ok, err = s.Apply(ctx, envOld, pOld)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, count(`SELECT count(*) FROM outbox`))
	var kwh float64
	var version int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT kwh, version FROM positions WHERE meter_id = 'm-1'`).Scan(&kwh, &version))
	require.Equal(t, 4.2, kwh)
	require.Equal(t, int64(5), version)

	// versión mayor: actualiza y añade un evento
	env2, p2 := decl(t, "m-1", 6, 1.5)
	ok, err = s.Apply(ctx, env2, p2)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, pool.QueryRow(ctx, `SELECT kwh, version FROM positions WHERE meter_id = 'm-1'`).Scan(&kwh, &version))
	require.Equal(t, 1.5, kwh)
	require.Equal(t, int64(6), version)
	require.Equal(t, 2, count(`SELECT count(*) FROM outbox`))

	// atomicidad: si la outbox falla (eventId repetido), la posición no cambia
	envClash, pClash := decl(t, "m-1", 7, 7.7)
	envClash.EventID = env2.EventID
	_, err = s.Apply(ctx, envClash, pClash)
	require.Error(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT kwh, version FROM positions WHERE meter_id = 'm-1'`).Scan(&kwh, &version))
	require.Equal(t, 1.5, kwh)
	require.Equal(t, int64(6), version)

	n, err := s.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestConcurrentApplySameMeterHasOneWinnerPerVersion(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	require.NoError(t, migrate.Up(ctx, pool, outbox.Migrations(), Migrations()))
	s := NewPositionStore(pool)

	const n = 20
	results := make(chan bool, n)
	for range n {
		go func() {
			env, p := decl(t, "m-1", 1, 1)
			ok, err := s.Apply(ctx, env, p)
			if err != nil {
				results <- false
				return
			}
			results <- ok
		}()
	}
	applied := 0
	for range n {
		if <-results {
			applied++
		}
	}
	require.Equal(t, 1, applied, "la misma versión se aplica una sola vez aunque lleguen a la vez")
}
