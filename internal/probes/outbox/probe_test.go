package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

func TestRunDrainsOutboxWithoutDuplicates(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	require.NoError(t, migrate.Up(ctx, pool, outbox.Migrations(), migrate.Source{Name: "s06", FS: migrationsFS, Dir: "migrations"}))

	// Siembra parcial previa (lo que quedó antes de un crash): la siembra continúa donde quedó.
	for i := 1; i <= 20; i++ {
		ok, err := seedRow(ctx, pool, i)
		require.NoError(t, err)
		require.True(t, ok)
	}
	ok, err := seedRow(ctx, pool, 5)
	require.NoError(t, err)
	require.False(t, ok, "fila ya sembrada")

	cfg := Config{Rows: 50, BatchSize: 10, Timeout: 30 * time.Second}
	svc := telemetry.New(telemetry.Info{Service: "outbox-probe"}, nil)
	res, err := runWithPool(ctx, pool, cfg, svc, failpoint.New())
	require.NoError(t, err)
	require.EqualValues(t, 50, res.Business)
	require.EqualValues(t, 50, res.Outbox)
	require.EqualValues(t, 0, res.Pending)
	require.EqualValues(t, 50, res.SinkDistinct)
	require.EqualValues(t, 0, res.Duplicates)
}
