package pricing

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func TestWalkIsBoundedDeterministicAndMoves(t *testing.T) {
	a, b := NewWalk(0.15, 0.01, 42), NewWalk(0.15, 0.01, 42)
	other := NewWalk(0.15, 0.01, 43)
	distinct := map[float64]bool{}
	differs := false
	for range 5000 {
		x := a.Next()
		require.Equal(t, x, b.Next(), "misma semilla, misma secuencia")
		differs = differs || x != other.Next()
		require.GreaterOrEqual(t, x, 0.10-1e-9)
		require.LessOrEqual(t, x, 0.20+1e-9)
		distinct[x] = true
	}
	require.True(t, differs)
	require.Greater(t, len(distinct), 20, "el precio se mueve")
}

func TestWalkZeroJitterStaysAtBase(t *testing.T) {
	w := NewWalk(0.2, 0, 1)
	for range 10 {
		require.Equal(t, 0.2, w.Next())
	}
}

func newFeed(t *testing.T, s Sender) (*Feed, *time.Time) {
	t.Helper()
	svc := telemetry.New(telemetry.Info{Service: ServiceName, Region: "r1"}, slog.New(slog.DiscardHandler))
	now := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	f := NewFeed("r1", NewWalk(0.15, 0.01, 1), s, svc, 5*time.Second, 15*time.Second, 1)
	f.Clock = func() time.Time { return now }
	return f, &now
}

type recorder struct {
	mu   sync.Mutex
	sent []amqpx.Outgoing
	errs []error // se consumen en orden
}

func (r *recorder) Send(_ context.Context, o amqpx.Outgoing) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			return err
		}
	}
	r.sent = append(r.sent, o)
	return nil
}

func (r *recorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.sent) }

func TestIntervalsStayInRange(t *testing.T) {
	f, _ := newFeed(t, &recorder{})
	lo, hi := time.Hour, time.Duration(0)
	for range 2000 {
		d := f.Interval()
		require.GreaterOrEqual(t, d, 5*time.Second)
		require.LessOrEqual(t, d, 15*time.Second)
		lo, hi = min(lo, d), max(hi, d)
	}
	require.Less(t, lo, 6*time.Second)
	require.Greater(t, hi, 14*time.Second)

	f.MinInterval, f.MaxInterval = 3*time.Second, 3*time.Second
	require.Equal(t, 3*time.Second, f.Interval())
}

func TestEventHasMonotonicVersionAndValidity(t *testing.T) {
	f, now := newFeed(t, &recorder{})
	e1, err := f.NextEvent()
	require.NoError(t, err)
	e2, err := f.NextEvent() // el reloj no avanzó
	require.NoError(t, err)
	require.Equal(t, now.UnixMilli(), e1.Version)
	require.Equal(t, e1.Version+1, e2.Version, "estrictamente creciente aunque el reloj no avance")
	*now = now.Add(time.Hour)
	e3, _ := f.NextEvent()
	require.Equal(t, now.UnixMilli(), e3.Version)

	p, err := events.DecodePayload[events.PriceUpdated](e1)
	require.NoError(t, err)
	require.Equal(t, "r1", p.Region)
	require.Equal(t, time.Date(2026, 10, 16, 10, 0, 30, 0, time.UTC), p.ValidUntil, "ahora + 2 x intervalo máximo")
	require.Equal(t, "r1", e1.AggregateID)
	require.Len(t, events.TraceID(e1.Traceparent), 32)
	require.NotEqual(t, e1.EventID, e2.EventID)
}

func TestPublishOnceSendsToPricesExchange(t *testing.T) {
	rec := &recorder{}
	f, _ := newFeed(t, rec)
	env, err := f.PublishOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, rec.count())
	o := rec.sent[0]
	require.Equal(t, "kwh-prices.x", o.Exchange)
	require.Equal(t, "kwh-prices", o.RoutingKey)
	require.Equal(t, env.Traceparent, o.Headers["traceparent"])
	require.Equal(t, env.EventID, o.Headers["eventId"])
	require.Equal(t, events.TypePriceUpdated, o.Headers["type"])
	back, err := events.Unmarshal(o.Body)
	require.NoError(t, err)
	require.Equal(t, env.EventID, back.EventID)
}

func TestRunPublishesImmediatelyAndRetriesAfterFailure(t *testing.T) {
	rec := &recorder{errs: []error{errors.New("broker down")}}
	f, _ := newFeed(t, rec)
	f.MinInterval, f.MaxInterval = 10*time.Millisecond, 10*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	require.Eventually(t, func() bool { return rec.count() >= 3 }, 5*time.Second, 5*time.Millisecond, "el primer fallo no detiene el bucle")
	cancel()
	require.NoError(t, <-done)
}

func TestConfigFromEnv(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	base := map[string]string{"REGION": "r1", "AMQP_USER": "price-feed-r1", "AMQP_PASSWORD": "x"}
	c, err := ConfigFromEnv(get(base))
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, c.MinInterval)
	require.Equal(t, 15*time.Second, c.MaxInterval)
	require.Equal(t, 0.15, c.Base)
	require.Equal(t, 0.01, c.Jitter)
	require.Equal(t, int64(1), c.Seed)

	bad := map[string]string{"PRICE_MIN_INTERVAL_S": "20", "PRICE_MAX_INTERVAL_S": "10", "PRICE_BASE": "0.02", "PRICE_JITTER": "0.01"}
	for k, v := range base {
		bad[k] = v
	}
	_, err = ConfigFromEnv(get(bad))
	require.ErrorContains(t, err, "PRICE_MAX_INTERVAL_S")
	require.ErrorContains(t, err, "PRICE_JITTER")

	_, err = ConfigFromEnv(get(map[string]string{}))
	require.ErrorContains(t, err, "REGION")
	require.ErrorContains(t, err, "AMQP_USER")
}
