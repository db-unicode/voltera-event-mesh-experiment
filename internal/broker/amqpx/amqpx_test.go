package amqpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestConfigFromEnv(t *testing.T) {
	c, err := ConfigFromEnv(env(map[string]string{"AMQP_USER": "u", "AMQP_PASSWORD": "p", "REGION": "r1"}))
	require.NoError(t, err)
	require.Equal(t, Config{Host: "rabbitmq.voltera-infra.svc", Port: 5672, User: "u", Password: "p", Vhost: "r1"}, c)
	require.Equal(t, "amqp://u:p@rabbitmq.voltera-infra.svc/r1", c.URI())

	c, err = ConfigFromEnv(env(map[string]string{"AMQP_USER": "u", "AMQP_PASSWORD": "p@/x", "AMQP_VHOST": "/", "AMQP_PORT": "5673", "AMQP_HOST": "h"}))
	require.NoError(t, err)
	got, err := ConfigFromURI(c.URI())
	require.NoError(t, err)
	require.Equal(t, c, got, "la URI escapa y recupera contraseña y vhost")

	_, err = ConfigFromEnv(env(map[string]string{"AMQP_PORT": "x"}))
	require.ErrorContains(t, err, "AMQP_PORT")
	require.ErrorContains(t, err, "AMQP_USER")
	require.ErrorContains(t, err, "AMQP_PASSWORD")
	require.ErrorContains(t, err, "AMQP_VHOST")
}

func TestEnvelopeOutgoing(t *testing.T) {
	e, err := events.NewPriceUpdated("r1", 7, time.Now(), events.PriceUpdated{Region: "r1", PricePerKWh: 0.1, ValidUntil: time.Now()})
	require.NoError(t, err)
	e = e.WithTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	o := EnvelopeOutgoing("kwh-prices", e, []byte("{}"))
	require.Equal(t, "kwh-prices.x", o.Exchange)
	require.Equal(t, "kwh-prices", o.RoutingKey)
	require.Equal(t, e.EventID, o.MessageID)
	require.Equal(t, map[string]any{"eventId": e.EventID, "type": events.TypePriceUpdated, "traceparent": e.Traceparent}, o.Headers)

	e.Traceparent = ""
	require.NotContains(t, EnvelopeOutgoing("kwh-prices", e, nil).Headers, "traceparent")
}

func TestBackoff(t *testing.T) {
	min, max := 100*time.Millisecond, time.Second
	require.Equal(t, 100*time.Millisecond, Backoff(min, max, 0))
	require.Equal(t, 100*time.Millisecond, Backoff(min, max, 1))
	require.Equal(t, 200*time.Millisecond, Backoff(min, max, 2))
	require.Equal(t, 800*time.Millisecond, Backoff(min, max, 4))
	require.Equal(t, time.Second, Backoff(min, max, 5))
	require.Equal(t, time.Second, Backoff(min, max, 50))
}

func TestDeliveryHeaderAndInvalid(t *testing.T) {
	d := Delivery{Headers: amqp.Table{"a": "x", "b": []byte("y"), "c": 3}}
	require.Equal(t, "x", d.Header("a"))
	require.Equal(t, "y", d.Header("b"))
	require.Empty(t, d.Header("c"))
	require.Empty(t, d.Header("zz"))

	base := errors.New("boom")
	err := fmt.Errorf("wrapped: %w", InvalidAs("X", base))
	require.True(t, isInvalid(err))
	require.ErrorIs(t, err, base)
	require.False(t, isInvalid(base))
}

func TestNewConsumerValidates(t *testing.T) {
	_, err := NewConsumer(ConsumerConfig{})
	require.Error(t, err)
	c, err := NewConsumer(ConsumerConfig{Queue: "q"})
	require.NoError(t, err)
	require.Equal(t, 10, c.cfg.Prefetch)
	require.Equal(t, 1, c.cfg.Concurrency)
	require.Equal(t, "unknown", c.cfg.EventType)
	require.Error(t, c.Ready(context.Background()))
}

// ---- integración con RabbitMQ real (TEST_AMQP_URL=amqp://user:pass@host:port/vhost) ----

type rig struct {
	cfg   Config
	queue string
	raw   *amqp.Channel
}

// newRig declara la cola <q>, el exchange direct <q>.x y su binding, y los borra al terminar.
func newRig(t *testing.T) *rig {
	t.Helper()
	uri := os.Getenv("TEST_AMQP_URL")
	if uri == "" {
		t.Skip("TEST_AMQP_URL no definido: integración con RabbitMQ omitida")
	}
	cfg, err := ConfigFromURI(uri)
	require.NoError(t, err)
	conn, err := cfg.dial()
	require.NoError(t, err)
	ch, err := conn.Channel()
	require.NoError(t, err)
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	q := "amqpx-test-" + hex.EncodeToString(b)
	_, err = ch.QueueDeclare(q, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare(q+".x", "direct", false, false, false, false, nil))
	require.NoError(t, ch.QueueBind(q, q, q+".x", false, nil))
	t.Cleanup(func() {
		_, _ = ch.QueueDelete(q, false, false, false)
		_ = ch.ExchangeDelete(q+".x", false, false)
		_ = conn.Close()
	})
	return &rig{cfg: cfg, queue: q, raw: ch}
}

func (r *rig) publisher(t *testing.T) *Publisher {
	t.Helper()
	p := NewPublisher(r.cfg, PublisherOptions{ConfirmTimeout: 5 * time.Second})
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func (r *rig) outgoing(id string) Outgoing {
	return Outgoing{Exchange: r.queue + ".x", RoutingKey: r.queue, Body: []byte(`{"id":"` + id + `"}`), MessageID: id,
		Headers: map[string]any{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "eventId": id, "type": "T"}}
}

// startConsumer arranca un Consumer y devuelve la función que lo para y espera a que termine.
func (r *rig) startConsumer(t *testing.T, cfg ConsumerConfig, h Handler) (*Consumer, func()) {
	t.Helper()
	cfg.Conn = r.cfg
	cfg.Queue = r.queue
	cfg.Logger = slog.New(slog.DiscardHandler)
	c, err := NewConsumer(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx, h) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("el consumer no terminó")
		}
	}
	t.Cleanup(stop)
	require.Eventually(t, func() bool { return c.Ready(ctx) == nil }, 10*time.Second, 20*time.Millisecond)
	return c, stop
}

func queueDepth(t *testing.T, r *rig) int {
	t.Helper()
	q, err := r.raw.QueueDeclarePassive(r.queue, false, true, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

func TestPublisherMessageProperties(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-1")))

	d, ok, err := r.raw.Get(r.queue, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, amqp.Persistent, d.DeliveryMode)
	require.Equal(t, "e-1", d.MessageId)
	require.Equal(t, "e-1", d.Headers["eventId"])
	require.Equal(t, "T", d.Headers["type"])
	require.Equal(t, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", d.Headers["traceparent"])
	require.JSONEq(t, `{"id":"e-1"}`, string(d.Body))
}

func TestPublisherUnroutable(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	o := r.outgoing("e-1")
	o.RoutingKey = "no-such-binding"
	err := p.Send(context.Background(), o)
	require.ErrorIs(t, err, ErrUnroutable)
	// el canal sigue sano: el siguiente mensaje bien encaminado pasa
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-2")))
	require.Equal(t, int64(0), p.Reconnects())
}

func TestPublisherConcurrentAndReconnect(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert := require.New(t)
			assert.NoError(p.Send(context.Background(), r.outgoing(fmt.Sprintf("a-%d", i))))
		}()
	}
	wg.Wait()
	require.Equal(t, 50, queueDepth(t, r))

	// la conexión cae: el siguiente Send abre una nueva
	p.mu.Lock()
	require.NoError(t, p.conn.Close())
	p.mu.Unlock()
	require.NoError(t, p.Send(context.Background(), r.outgoing("b-1")))
	require.Equal(t, int64(1), p.Reconnects())
	require.Equal(t, 51, queueDepth(t, r))
}

func TestPublisherClosed(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	require.NoError(t, p.Ping(context.Background()))
	require.NoError(t, p.Close())
	require.Error(t, p.Send(context.Background(), r.outgoing("x")))
}

func TestConsumerAcksOnlyAfterHandler(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-1")))

	var calls atomic.Int32
	redelivered := make(chan bool, 4)
	release := make(chan struct{})
	_, stop := r.startConsumer(t, ConsumerConfig{Prefetch: 1, BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond},
		func(_ context.Context, d Delivery) error {
			n := calls.Add(1)
			redelivered <- d.Redelivered
			if n == 1 {
				return errors.New("transient") // nack + requeue
			}
			<-release // mientras el handler corre, el mensaje sigue sin ack
			return nil
		})

	require.Equal(t, false, <-redelivered)
	require.Equal(t, true, <-redelivered, "tras un error transitorio vuelve a entregarse")
	q, err := r.raw.QueueDeclarePassive(r.queue, false, true, false, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, q.Messages, "en vuelo (unacked): no cuenta como listo")
	close(release)
	require.Eventually(t, func() bool {
		return queueDepth(t, r) == 0
	}, 5*time.Second, 20*time.Millisecond)
	stop()
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, 0, queueDepth(t, r), "al cerrar la conexión lo no acusado volvería a la cola: no vuelve nada")
}

func TestConsumerInvalidIsNotRequeued(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	reg := prometheus.NewRegistry()
	m := telemetry.NewMetrics(reg)

	var calls atomic.Int32
	_, stop := r.startConsumer(t, ConsumerConfig{Metrics: m, EventType: "MeterDeclaration"},
		func(context.Context, Delivery) error {
			calls.Add(1)
			return Invalid(errors.New("unknown vendor"))
		})
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-1")))
	require.Eventually(t, func() bool { return calls.Load() >= 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond) // un requeue erróneo habría vuelto a entregar
	stop()

	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, 0, queueDepth(t, r))
	require.Equal(t, 1.0, promtest.ToFloat64(m.EventsConsumed.WithLabelValues("MeterDeclaration", r.queue, "error")))
}

func TestConsumerReconnects(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	got := make(chan string, 8)
	c, _ := r.startConsumer(t, ConsumerConfig{}, func(_ context.Context, d Delivery) error {
		got <- d.MessageID
		return nil
	})
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-1")))
	require.Equal(t, "e-1", <-got)

	c.dropConnection()
	require.Eventually(t, func() bool { return c.Sessions() >= 2 && c.Ready(context.Background()) == nil },
		15*time.Second, 50*time.Millisecond)
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-2")))
	require.Equal(t, "e-2", <-got)
}

func TestConsumerPrefetchBoundsInFlight(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	for i := range 10 {
		require.NoError(t, p.Send(context.Background(), r.outgoing(fmt.Sprintf("e-%d", i))))
	}
	var cur, peak atomic.Int32
	var done atomic.Int32
	_, stop := r.startConsumer(t, ConsumerConfig{Prefetch: 3, Concurrency: 6}, func(context.Context, Delivery) error {
		n := cur.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cur.Add(-1)
		done.Add(1)
		return nil
	})
	require.Eventually(t, func() bool { return done.Load() == 10 }, 10*time.Second, 20*time.Millisecond)
	stop()
	require.Equal(t, int32(3), peak.Load(), "prefetch=3 limita los mensajes sin ack aunque haya 6 workers")
}

func TestConsumerShutdownFinishesInFlight(t *testing.T) {
	r := newRig(t)
	p := r.publisher(t)
	require.NoError(t, p.Send(context.Background(), r.outgoing("e-1")))
	started := make(chan struct{})
	var finished atomic.Bool
	_, stop := r.startConsumer(t, ConsumerConfig{}, func(ctx context.Context, _ Delivery) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		finished.Store(ctx.Err() == nil) // el handler no se cancela al apagar
		return nil
	})
	<-started
	stop()
	require.True(t, finished.Load())
	require.Equal(t, 0, queueDepth(t, r))
}
