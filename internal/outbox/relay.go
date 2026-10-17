package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// Nombres de los failpoints del relay.
const (
	FPAfterCommitBeforePublish = "after_commit_before_publish"
	FPAfterPublishBeforeMark   = "after_publish_before_mark"
)

// Message es lo que recibe el Publisher.
type Message struct {
	ID          int64
	EventID     string
	Type        string
	Topic       string
	Key         string
	Traceparent string // también debe ir en un header del mensaje
	// Body es el sobre JSON con committedAt (= outbox.created_at) y emittedAt ya fijados.
	Body     []byte
	Envelope events.Envelope
}

// Publisher entrega un mensaje al broker y vuelve solo cuando el broker confirmó
// (ack/confirm). Un error significa "no confirmado": el relay lo reintentará.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// PublisherFunc adapta una función a Publisher.
type PublisherFunc func(ctx context.Context, msg Message) error

// Publish implementa Publisher.
func (f PublisherFunc) Publish(ctx context.Context, msg Message) error { return f(ctx, msg) }

// Metrics son las métricas del relay.
type Metrics struct {
	Pending       prometheus.Gauge
	OldestPending prometheus.Gauge
	PublishTotal  *prometheus.CounterVec // result=ok|error
}

// NewMetrics crea outbox_pending, outbox_oldest_pending_age_seconds y
// outbox_publish_total{result} y los registra en reg (si no es nil).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending", Help: "Filas de la outbox con published_at IS NULL."}),
		OldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_oldest_pending_age_seconds", Help: "Edad de la fila pendiente más antigua."}),
		PublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_publish_total", Help: "Publicaciones del relay por resultado."}, []string{"result"}),
	}
	m.PublishTotal.WithLabelValues("ok")
	m.PublishTotal.WithLabelValues("error")
	if reg != nil {
		reg.MustRegister(m.Pending, m.OldestPending, m.PublishTotal)
	}
	return m
}

// Config configura el Relay. Todos los campos son opcionales.
type Config struct {
	BatchSize     int           // filas por lote (por defecto 100)
	PollInterval  time.Duration // espera cuando no hay trabajo (por defecto 100 ms)
	StatsInterval time.Duration // refresco de las gauges (por defecto 1 s)
	// Telemetry, si se da, aporta Logger y Registerer por defecto y cuenta events_published_total.
	Telemetry  *telemetry.Service
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Failpoints *failpoint.Registry
	Clock      func() time.Time // para emittedAt (por defecto time.Now)
}

// Relay publica las filas pendientes de la outbox en orden de id.
type Relay struct {
	pool    *pgxpool.Pool
	pub     Publisher
	cfg     Config
	log     *slog.Logger
	metrics *Metrics
	wake    chan struct{}
}

// NewRelay crea el relay sobre pool (la base del servicio dueño de la outbox).
func NewRelay(pool *pgxpool.Pool, pub Publisher, cfg Config) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.StatsInterval <= 0 {
		cfg.StatsInterval = time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Telemetry != nil {
		if cfg.Logger == nil {
			cfg.Logger = cfg.Telemetry.Logger
		}
		if cfg.Registerer == nil {
			cfg.Registerer = cfg.Telemetry.Reg
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Relay{pool: pool, pub: pub, cfg: cfg, log: cfg.Logger, metrics: NewMetrics(cfg.Registerer), wake: make(chan struct{}, 1)}
}

// Wake despierta al relay tras un commit para publicar sin esperar al siguiente poll.
// No bloquea.
func (r *Relay) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Stats es el estado de la outbox (también la sección "outbox" de /debug/state).
type Stats struct {
	Pending      int64   `json:"pending"`
	OldestAgeSec float64 `json:"oldestAgeSec"`
}

// Stats consulta cuántas filas están pendientes y la edad de la más antigua.
func (r *Relay) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := r.pool.QueryRow(ctx, `
		SELECT count(*), coalesce(extract(epoch FROM clock_timestamp() - min(created_at)), 0)::float8
		FROM outbox WHERE published_at IS NULL`).Scan(&s.Pending, &s.OldestAgeSec)
	if err != nil {
		return Stats{}, fmt.Errorf("outbox: stats: %w", err)
	}
	return s, nil
}

// StateProvider sirve para telemetry.Service.AddState("outbox", relay.StateProvider()).
func (r *Relay) StateProvider() func(context.Context) any {
	return func(ctx context.Context) any {
		s, err := r.Stats(ctx)
		if err != nil {
			return map[string]string{"error": err.Error()}
		}
		return s
	}
}

func (r *Relay) refreshStats(ctx context.Context) {
	s, err := r.Stats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("outbox stats failed", "stage", "publish", "result", "error", "error", err)
		}
		return
	}
	r.metrics.Pending.Set(float64(s.Pending))
	r.metrics.OldestPending.Set(s.OldestAgeSec)
}

// Run publica en bucle hasta que ctx se cancele (devuelve nil en ese caso).
func (r *Relay) Run(ctx context.Context) error {
	lastStats := time.Time{}
	for ctx.Err() == nil {
		n, err := r.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			r.log.Error("outbox relay iteration failed", "stage", "publish", "result", "error", "error", err)
		}
		if time.Since(lastStats) >= r.cfg.StatsInterval {
			r.refreshStats(ctx)
			lastStats = time.Now()
		}
		if err == nil && n >= r.cfg.BatchSize {
			continue // probablemente queda más trabajo
		}
		select {
		case <-ctx.Done():
		case <-r.wake:
		case <-time.After(r.cfg.PollInterval):
		}
	}
	return nil
}

type row struct {
	id        int64
	eventID   string
	topic     string
	key       string
	payload   []byte
	createdAt time.Time
}

// RunOnce procesa un lote: bloquea hasta BatchSize filas pendientes (FOR UPDATE SKIP
// LOCKED, varios relays no se pisan), publica cada una en orden y marca published_at.
// Devuelve cuántas publicó. Ante un error de publicación marca las ya publicadas y
// devuelve el error (el resto queda pendiente, conservando el orden).
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, `
		SELECT id, event_id::text, topic, key, payload, created_at
		FROM outbox WHERE published_at IS NULL
		ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox: select: %w", err)
	}
	batch, err := pgx.CollectRows(rows, func(rw pgx.CollectableRow) (row, error) {
		var x row
		return x, rw.Scan(&x.id, &x.eventID, &x.topic, &x.key, &x.payload, &x.createdAt)
	})
	if err != nil {
		return 0, fmt.Errorf("outbox: scan: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	done := make([]int64, 0, len(batch))
	var pubErr error
	for _, x := range batch {
		if err := r.publishOne(ctx, x); err != nil {
			pubErr = err
			break
		}
		done = append(done, x.id)
	}

	if len(done) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY($1)`, done); err != nil {
			return 0, fmt.Errorf("outbox: mark published: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("outbox: commit: %w", err)
		}
	}
	return len(done), pubErr
}

func (r *Relay) publishOne(ctx context.Context, x row) error {
	env, err := events.Unmarshal(x.payload)
	if err != nil {
		r.metrics.PublishTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("outbox: row %d has an invalid envelope: %w", x.id, err)
	}
	// committedAt es el sello de la fila; emittedAt se reescribe en cada intento.
	env.CommittedAt = x.createdAt.UTC()
	env.EmittedAt = r.cfg.Clock().UTC()
	body, err := events.Marshal(env)
	if err != nil {
		r.metrics.PublishTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("outbox: row %d: %w", x.id, err)
	}

	if r.cfg.Failpoints != nil {
		r.cfg.Failpoints.Hit(ctx, FPAfterCommitBeforePublish)
	}
	msg := Message{ID: x.id, EventID: x.eventID, Type: env.Type, Topic: x.topic, Key: x.key,
		Traceparent: env.Traceparent, Body: body, Envelope: env}
	if err := r.pub.Publish(ctx, msg); err != nil {
		r.metrics.PublishTotal.WithLabelValues("error").Inc()
		telemetry.LogStage(r.log, telemetry.StagePublish, env, telemetry.ResultError, "topic", x.topic, "error", err)
		return fmt.Errorf("outbox: publish %s: %w", x.eventID, err)
	}
	r.metrics.PublishTotal.WithLabelValues("ok").Inc()
	if r.cfg.Telemetry != nil {
		r.cfg.Telemetry.Published(env, x.topic)
	}
	telemetry.LogStage(r.log, telemetry.StagePublish, env, telemetry.ResultOK, "topic", x.topic)
	if r.cfg.Failpoints != nil {
		r.cfg.Failpoints.Hit(ctx, FPAfterPublishBeforeMark)
	}
	return nil
}

