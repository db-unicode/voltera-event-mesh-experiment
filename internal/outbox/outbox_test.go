package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.Postgres(t)
	require.NoError(t, migrate.Up(context.Background(), pool, Migrations()))
	return pool
}

func newEvent(t *testing.T, i int) events.Envelope {
	t.Helper()
	e, err := events.NewPositionDeclared("r1", int64(i), time.Now(),
		events.PositionDeclared{MeterID: fmt.Sprintf("m-%04d", i), Side: events.SideSurplus, KWh: 1, Zone: "z1"})
	require.NoError(t, err)
	return e.WithTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
}

func appendN(t *testing.T, pool *pgxpool.Pool, from, n int) []events.Envelope {
	t.Helper()
	ctx := context.Background()
	out := make([]events.Envelope, 0, n)
	for i := from; i < from+n; i++ {
		e := newEvent(t, i)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, Append(ctx, tx, e, "energy-positions", e.AggregateID))
		require.NoError(t, tx.Commit(ctx))
		out = append(out, e)
	}
	return out
}

type recorder struct {
	mu   sync.Mutex
	msgs []Message
	fail func(Message) error
}

func (r *recorder) Publish(_ context.Context, m Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		if err := r.fail(m); err != nil {
			return err
		}
	}
	r.msgs = append(r.msgs, m)
	return nil
}

func (r *recorder) ids() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.msgs))
	for i, m := range r.msgs {
		out[i] = m.EventID
	}
	return out
}

func pending(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&n))
	return n
}

func TestAppendIsAtomicWithBusinessTx(t *testing.T) {
	pool := setup(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "CREATE TABLE positions (meter_id text PRIMARY KEY, version bigint)")
	require.NoError(t, err)

	// Rollback: ni estado ni evento.
	e := newEvent(t, 1)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO positions VALUES ($1, 1)", e.AggregateID)
	require.NoError(t, err)
	require.NoError(t, Append(ctx, tx, e, "t", e.AggregateID))
	require.NoError(t, tx.Rollback(ctx))
	var pos, out int
	require.NoError(t, pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM positions), (SELECT count(*) FROM outbox)").Scan(&pos, &out))
	require.Equal(t, 0, pos+out)

	// Commit: los dos.
	tx, err = pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO positions VALUES ($1, 1)", e.AggregateID)
	require.NoError(t, err)
	require.NoError(t, Append(ctx, tx, e, "t", e.AggregateID))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM positions), (SELECT count(*) FROM outbox)").Scan(&pos, &out))
	require.Equal(t, 2, pos+out)

	// El mismo eventId dos veces es un error.
	require.Error(t, Append(ctx, pool, e, "t", "k"))
	require.Error(t, Append(ctx, pool, newEvent(t, 2), "", "k"), "topic vacío")
	require.Error(t, Append(ctx, pool, events.Envelope{}, "t", "k"), "sobre inválido")
}

func TestRelayPublishesInOrderAndMarks(t *testing.T) {
	pool := setup(t)
	evs := appendN(t, pool, 0, 25)
	rec := &recorder{}
	reg := prometheus.NewRegistry()
	fixed := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	r := NewRelay(pool, rec, Config{BatchSize: 10, Registerer: reg, Clock: func() time.Time { return fixed }})

	for _, want := range []int{10, 10, 5, 0} {
		n, err := r.RunOnce(context.Background())
		require.NoError(t, err)
		require.Equal(t, want, n)
	}
	require.Equal(t, 0, pending(t, pool))
	require.Len(t, rec.msgs, 25)
	for i, m := range rec.msgs {
		require.Equal(t, evs[i].EventID, m.EventID, "orden por id")
		require.Equal(t, "energy-positions", m.Topic)
		require.Equal(t, evs[i].AggregateID, m.Key)
		require.Equal(t, evs[i].Traceparent, m.Traceparent)

		got, err := events.Unmarshal(m.Body)
		require.NoError(t, err)
		require.True(t, fixed.Equal(got.EmittedAt), "emittedAt lo fija el relay")
		require.False(t, got.CommittedAt.IsZero(), "committedAt = created_at")
		require.True(t, got.CommittedAt.Before(time.Now()))
		require.JSONEq(t, string(evs[i].Payload), string(got.Payload))
	}
	require.Equal(t, 25.0, promtest.ToFloat64(r.metrics.PublishTotal.WithLabelValues("ok")))
}

func TestRelayStopsOnPublishErrorAndResumes(t *testing.T) {
	pool := setup(t)
	evs := appendN(t, pool, 0, 10)
	rec := &recorder{fail: func(m Message) error {
		if m.EventID == evs[6].EventID {
			return errors.New("broker down")
		}
		return nil
	}}
	r := NewRelay(pool, rec, Config{BatchSize: 100, Registerer: prometheus.NewRegistry()})

	n, err := r.RunOnce(context.Background())
	require.ErrorContains(t, err, "broker down")
	require.Equal(t, 6, n)
	require.Equal(t, 4, pending(t, pool), "las 6 anteriores quedan marcadas; el resto pendiente")
	require.Equal(t, 1.0, promtest.ToFloat64(r.metrics.PublishTotal.WithLabelValues("error")))

	rec.fail = nil
	n, err = r.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, 0, pending(t, pool))
	ids := rec.ids()
	require.Len(t, ids, 10)
	for i, e := range evs {
		require.Equal(t, e.EventID, ids[i])
	}
}

func TestConcurrentRelaysDoNotDuplicate(t *testing.T) {
	pool := setup(t)
	appendN(t, pool, 0, 300)
	rec := &recorder{}
	var wg sync.WaitGroup
	for range 3 {
		r := NewRelay(pool, rec, Config{BatchSize: 20, Registerer: prometheus.NewRegistry()})
		wg.Go(func() {
			for {
				n, err := r.RunOnce(context.Background())
				require.NoError(t, err)
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()
	ids := rec.ids()
	require.Len(t, ids, 300)
	require.Len(t, uniq(ids), 300, "SKIP LOCKED: cada fila la toma un solo relay")
	require.Equal(t, 0, pending(t, pool))
}

func uniq(in []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, s := range in {
		m[s] = struct{}{}
	}
	return m
}

type crashed struct{}

// runCatchingCrash ejecuta RunOnce y devuelve true si un failpoint "mató" el proceso.
func runCatchingCrash(r *Relay) (crash bool) {
	defer func() {
		if v := recover(); v != nil {
			if _, ok := v.(crashed); !ok {
				panic(v)
			}
			crash = true
		}
	}()
	_, _ = r.RunOnce(context.Background())
	return false
}

func TestFailpointAfterCommitBeforePublishLosesNothing(t *testing.T) {
	pool := setup(t)
	appendN(t, pool, 0, 30)
	rec := &recorder{}
	fp := failpoint.New(failpoint.WithExit(func(int) { panic(crashed{}) }))
	require.NoError(t, fp.Load(FPAfterCommitBeforePublish+"=crash-after:12"))
	r := NewRelay(pool, rec, Config{BatchSize: 10, Failpoints: fp, Registerer: prometheus.NewRegistry()})

	require.False(t, runCatchingCrash(r), "lote 1: hits 1-10")
	require.True(t, runCatchingCrash(r), "lote 2: el hit 13 mata el proceso antes de publicar la fila 13")
	require.Equal(t, 20, pending(t, pool), "el lote 2 no se marcó")
	require.Len(t, rec.ids(), 12)

	// "Reinicio": relay nuevo (failpoint ya agotado).
	r2 := NewRelay(pool, rec, Config{BatchSize: 10, Failpoints: fp, Registerer: prometheus.NewRegistry()})
	for {
		n, err := r2.RunOnce(context.Background())
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	require.Equal(t, 0, pending(t, pool))
	require.Len(t, uniq(rec.ids()), 30)
}

func TestFailpointAfterPublishBeforeMarkDuplicates(t *testing.T) {
	pool := setup(t)
	appendN(t, pool, 0, 30)
	rec := &recorder{}
	fp := failpoint.New(failpoint.WithExit(func(int) { panic(crashed{}) }))
	require.NoError(t, fp.Load(FPAfterPublishBeforeMark+"=crash-after:12"))
	r := NewRelay(pool, rec, Config{BatchSize: 10, Failpoints: fp, Registerer: prometheus.NewRegistry()})

	require.False(t, runCatchingCrash(r))
	require.True(t, runCatchingCrash(r), "crash tras publicar la fila 13 y antes de marcar el lote")
	require.Len(t, rec.ids(), 13)

	for {
		n, err := r.RunOnce(context.Background())
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	ids := rec.ids()
	require.Equal(t, 0, pending(t, pool))
	require.Len(t, uniq(ids), 30, "ninguna se pierde")
	require.Greater(t, len(ids), 30, "hay duplicados (al-menos-una-vez)")
	require.Len(t, ids, 33)
}

func TestStatsAndRunLoop(t *testing.T) {
	pool := setup(t)
	rec := &recorder{}
	reg := prometheus.NewRegistry()
	r := NewRelay(pool, rec, Config{BatchSize: 50, PollInterval: 20 * time.Millisecond, StatsInterval: 20 * time.Millisecond, Registerer: reg})

	st, err := r.Stats(context.Background())
	require.NoError(t, err)
	require.Equal(t, Stats{}, st)

	appendN(t, pool, 0, 5)
	time.Sleep(50 * time.Millisecond)
	st, err = r.Stats(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 5, st.Pending)
	require.Greater(t, st.OldestAgeSec, 0.04)
	require.EqualValues(t, 5, r.StateProvider()(context.Background()).(Stats).Pending)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	require.Eventually(t, func() bool { return pending(t, pool) == 0 }, 5*time.Second, 20*time.Millisecond)
	appendN(t, pool, 100, 3)
	r.Wake()
	require.Eventually(t, func() bool { return len(rec.ids()) == 8 }, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return promtest.ToFloat64(r.metrics.Pending) == 0 }, 5*time.Second, 20*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run no terminó al cancelar")
	}
}
