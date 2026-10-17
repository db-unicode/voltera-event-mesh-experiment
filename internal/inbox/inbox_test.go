package inbox

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

func TestTryMark(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	require.NoError(t, migrate.Up(ctx, pool, Migrations()))

	fresh, err := TryMark(ctx, pool, "energy-transfer", "match-1")
	require.NoError(t, err)
	require.True(t, fresh)
	fresh, err = TryMark(ctx, pool, "energy-transfer", "match-1")
	require.NoError(t, err)
	require.False(t, fresh, "segunda vez = duplicado")

	fresh, err = TryMark(ctx, pool, "billing", "match-1")
	require.NoError(t, err)
	require.True(t, fresh, "otro consumidor, otra marca")
	fresh, err = TryMark(ctx, pool, "energy-transfer", "match-2")
	require.NoError(t, err)
	require.True(t, fresh)
}

func TestTryMarkRollsBackWithTransaction(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	require.NoError(t, migrate.Up(ctx, pool, Migrations()))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	fresh, err := TryMark(ctx, tx, "c", "e1")
	require.NoError(t, err)
	require.True(t, fresh)
	require.NoError(t, tx.Rollback(ctx))

	// El efecto no se aplicó (rollback): el reintento debe verse como nuevo.
	fresh, err = TryMark(ctx, pool, "c", "e1")
	require.NoError(t, err)
	require.True(t, fresh)
}

func TestTryMarkConcurrentOnlyOneWins(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	require.NoError(t, migrate.Up(ctx, pool, Migrations()))

	results := make(chan bool, 20)
	for range 20 {
		go func() {
			fresh, err := TryMark(ctx, pool, "c", "same")
			if err != nil {
				fresh = false
			}
			results <- fresh
		}()
	}
	wins := 0
	for range 20 {
		if <-results {
			wins++
		}
	}
	require.Equal(t, 1, wins)
}
