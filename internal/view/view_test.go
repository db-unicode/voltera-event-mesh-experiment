package view

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	itest "github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

func newStore(t *testing.T) (*Store, string, *prometheus.Registry) {
	t.Helper()
	rdb, prefix := itest.Redis(t)
	reg := prometheus.NewRegistry()
	return New(rdb, reg), prefix, reg
}

func TestUpsertVersionOrdering(t *testing.T) {
	s, p, reg := newStore(t)
	ctx := context.Background()
	key := p + "m-1"

	res, err := s.Upsert(ctx, key, 5, map[string]any{"side": "SURPLUS", "kwh": 4.2}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, Applied, res)

	res, err = s.Upsert(ctx, key, 3, map[string]any{"side": "DEFICIT", "kwh": 9}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, Stale, res)

	res, err = s.Upsert(ctx, key, 5, map[string]any{"side": "DEFICIT"}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, Stale, res, "misma versión = duplicado")

	e, err := s.Get(ctx, key)
	require.NoError(t, err)
	require.EqualValues(t, 5, e.Version)
	require.Equal(t, map[string]string{"side": "SURPLUS", "kwh": "4.2"}, e.Fields)

	res, err = s.Upsert(ctx, key, 6, map[string]any{"side": "DEFICIT"}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, Applied, res)
	e, err = s.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"side": "DEFICIT"}, e.Fields, "el estado nuevo reemplaza al anterior")

	require.Equal(t, 2.0, testutil.ToFloat64(s.metrics.Writes.WithLabelValues("applied")))
	require.Equal(t, 2.0, testutil.ToFloat64(s.metrics.Writes.WithLabelValues("stale")))
	_, err = reg.Gather()
	require.NoError(t, err)
}

func TestStaleDoesNotRefreshTTL(t *testing.T) {
	s, p, _ := newStore(t)
	ctx := context.Background()
	key := p + "m-ttl"
	_, err := s.Upsert(ctx, key, 2, map[string]any{"a": 1}, 3*time.Second)
	require.NoError(t, err)
	time.Sleep(1200 * time.Millisecond)
	res, err := s.Upsert(ctx, key, 1, map[string]any{"a": 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, Stale, res)
	e, err := s.Get(ctx, key)
	require.NoError(t, err)
	require.Less(t, e.TTL, 2200*time.Millisecond)
}

func TestTTLAndExpiry(t *testing.T) {
	s, p, _ := newStore(t)
	ctx := context.Background()
	key := p + "m-exp"
	_, err := s.Upsert(ctx, key, 1, map[string]any{"a": 1}, 2*time.Second)
	require.NoError(t, err)
	e, err := s.Get(ctx, key)
	require.NoError(t, err)
	require.InDelta(t, 2.0, e.TTL.Seconds(), 1.0)

	require.Eventually(t, func() bool {
		_, err := s.Get(ctx, key)
		return err == ErrNotFound
	}, 4*time.Second, 100*time.Millisecond)
}

func TestUpsertValidation(t *testing.T) {
	s, p, _ := newStore(t)
	ctx := context.Background()
	_, err := s.Upsert(ctx, p+"x", 1, map[string]any{"a": 1}, 0)
	require.Error(t, err)
	_, err = s.Upsert(ctx, p+"x", 1, map[string]any{"_v": 1}, time.Second)
	require.Error(t, err)
	_, err = s.Get(ctx, p+"missing")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestConcurrentUpsertKeepsHighestVersion(t *testing.T) {
	s, p, _ := newStore(t)
	ctx := context.Background()
	key := p + "m-race"
	var wg sync.WaitGroup
	for v := int64(1); v <= 50; v++ {
		wg.Go(func() {
			_, err := s.Upsert(ctx, key, v, map[string]any{"v": v}, time.Minute)
			require.NoError(t, err)
		})
	}
	wg.Wait()
	e, err := s.Get(ctx, key)
	require.NoError(t, err)
	require.EqualValues(t, 50, e.Version)
	require.Equal(t, "50", e.Fields["v"])
}

func TestScanByPrefix(t *testing.T) {
	s, p, _ := newStore(t)
	ctx := context.Background()
	for i := range 25 {
		_, err := s.Upsert(ctx, fmt.Sprintf("%ssurplus:z1:m-%02d", p, i), 1, map[string]any{"kwh": i}, time.Minute)
		require.NoError(t, err)
	}
	_, err := s.Upsert(ctx, p+"deficit:z1:m-99", 1, map[string]any{"kwh": 1}, time.Minute)
	require.NoError(t, err)

	got, err := s.ScanByPrefix(ctx, p+"surplus:")
	require.NoError(t, err)
	require.Len(t, got, 25)
	for _, e := range got {
		require.EqualValues(t, 1, e.Version)
		require.Greater(t, e.TTL, 0*time.Second)
		require.Contains(t, e.Fields, "kwh")
	}
	none, err := s.ScanByPrefix(ctx, p+"nothing:")
	require.NoError(t, err)
	require.Empty(t, none)

	// Los caracteres glob en el prefijo se tratan literalmente.
	lit, err := s.ScanByPrefix(ctx, p+"*")
	require.NoError(t, err)
	require.Empty(t, lit)
}
