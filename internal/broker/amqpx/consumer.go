package amqpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// Delivery es un mensaje entregado al Handler.
type Delivery struct {
	Queue       string
	Body        []byte
	Headers     amqp.Table
	MessageID   string
	Redelivered bool
	Timestamp   time.Time
}

// Header devuelve el header h como string ("" si no existe o no es texto).
func (d Delivery) Header(h string) string {
	switch v := d.Headers[h].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return ""
}

// Handler procesa un mensaje. nil ⇒ ack. Un error cualquiera es transitorio (nack con
// requeue tras un backoff); Invalid(err) marca el mensaje como inválido (nack sin requeue).
type Handler func(ctx context.Context, d Delivery) error

// InvalidError marca un mensaje que nunca podrá procesarse (formato desconocido, JSON roto):
// reintentarlo no sirve.
type InvalidError struct {
	Err error
	// EventType etiqueta events_consumed_total{result="error"} (por defecto, el del Consumer).
	EventType string
}

func (e *InvalidError) Error() string { return "invalid message: " + e.Err.Error() }
func (e *InvalidError) Unwrap() error { return e.Err }

// Invalid envuelve err como mensaje inválido.
func Invalid(err error) error { return &InvalidError{Err: err} }

// InvalidAs es como Invalid pero fija el event_type de la métrica.
func InvalidAs(eventType string, err error) error {
	return &InvalidError{Err: err, EventType: eventType}
}

// ConsumerConfig configura un Consumer.
type ConsumerConfig struct {
	Conn  Config
	Queue string
	// Prefetch es el qos del canal (por defecto 10).
	Prefetch int
	// Concurrency son los workers que comparten el canal (por defecto 1: la capacidad de
	// una réplica es 1/tiempo-de-proceso, base del dimensionado de KEDA).
	Concurrency int
	// BackoffMin y BackoffMax acotan la espera antes de devolver a la cola un mensaje con
	// error transitorio (exponencial; por defecto 100 ms y 5 s).
	BackoffMin, BackoffMax time.Duration
	// HandlerTimeout acota un Handler (por defecto 30 s). El Handler no se cancela al
	// apagar: termina el mensaje en curso.
	HandlerTimeout time.Duration
	// EventType y Metrics cuentan events_consumed_total{result="error"} de los inválidos.
	EventType string
	Metrics   *telemetry.Metrics
	Logger    *slog.Logger
}

const (
	defaultPrefetch       = 10
	defaultBackoffMin     = 100 * time.Millisecond
	defaultBackoffMax     = 5 * time.Second
	defaultHandlerTimeout = 30 * time.Second
	reconnectMin          = time.Second
	reconnectMax          = 10 * time.Second
)

// Consumer consume una cola con ack manual: el ack se envía solo después de que el
// Handler devuelve nil. Reconecta si cae la conexión o el canal.
type Consumer struct {
	cfg ConsumerConfig
	log *slog.Logger

	connected atomic.Bool
	failures  atomic.Int64 // errores transitorios consecutivos
	sessions  atomic.Int64

	mu   sync.Mutex
	conn *amqp.Connection
}

// NewConsumer valida cfg y crea el Consumer (no conecta hasta Run).
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Queue == "" {
		return nil, errors.New("amqpx: consumer needs a queue")
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = defaultPrefetch
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = defaultBackoffMin
	}
	if cfg.BackoffMax < cfg.BackoffMin {
		cfg.BackoffMax = max(defaultBackoffMax, cfg.BackoffMin)
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = defaultHandlerTimeout
	}
	if cfg.EventType == "" {
		cfg.EventType = "unknown"
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{cfg: cfg, log: log.With("queue", cfg.Queue)}, nil
}

// Ready devuelve nil mientras hay un consumidor activo (sirve de readiness check).
func (c *Consumer) Ready(context.Context) error {
	if !c.connected.Load() {
		return fmt.Errorf("amqpx: not consuming %q", c.cfg.Queue)
	}
	return nil
}

// Sessions cuenta las sesiones (conexión + canal + consumo) abiertas hasta ahora.
func (c *Consumer) Sessions() int64 { return c.sessions.Load() }

// Run consume hasta que ctx se cancele (devuelve nil en ese caso), reconectando con
// backoff exponencial. Al cancelar deja de pedir mensajes y termina los que están en curso.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	wait := reconnectMin
	for ctx.Err() == nil {
		consumed, err := c.session(ctx, h)
		c.connected.Store(false)
		if ctx.Err() != nil {
			return nil
		}
		if consumed {
			wait = reconnectMin
		}
		c.log.Warn("amqp consumer session ended, reconnecting", "stage", "consume", "result", "error", "error", err, "backoff", wait.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		wait = min(wait*2, reconnectMax)
	}
	return nil
}

// session abre conexión y canal y procesa entregas hasta que se cierren. consumed=true
// si llegó a consumir (reinicia el backoff de reconexión).
func (c *Consumer) session(ctx context.Context, h Handler) (consumed bool, err error) {
	cc := c.cfg.Conn
	if cc.ConnectionName == "" {
		cc.ConnectionName = "consumer-" + c.cfg.Queue
	}
	conn, err := cc.dial()
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("amqpx: open channel: %w", err)
	}
	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		return false, fmt.Errorf("amqpx: qos: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	tag := fmt.Sprintf("%s-%d", c.cfg.Queue, time.Now().UnixNano())
	deliveries, err := ch.Consume(c.cfg.Queue, tag, false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("amqpx: consume %q: %w", c.cfg.Queue, err)
	}
	c.sessions.Add(1)
	c.connected.Store(true)
	c.log.Info("amqp consumer started", "stage", "consume", "vhost", cc.Vhost, "prefetch", c.cfg.Prefetch, "concurrency", c.cfg.Concurrency)

	// Al cancelar ctx se cancela el consumidor: las entregas ya en vuelo se procesan y
	// luego el canal de entregas se cierra.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = ch.Cancel(tag, false)
		case <-stop:
		}
	}()

	var wg sync.WaitGroup
	for range c.cfg.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range deliveries {
				c.handle(ctx, h, d)
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return true, nil
	}
	select {
	case ce := <-closed:
		if ce != nil {
			return true, fmt.Errorf("amqpx: channel closed: %w", ce)
		}
	default:
	}
	return true, errors.New("amqpx: deliveries channel closed")
}

// handle ejecuta el Handler y decide ack / nack requeue / nack sin requeue.
func (c *Consumer) handle(ctx context.Context, h Handler, d amqp.Delivery) {
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.HandlerTimeout)
	defer cancel()
	err := h(hctx, Delivery{
		Queue: c.cfg.Queue, Body: d.Body, Headers: d.Headers, MessageID: d.MessageId,
		Redelivered: d.Redelivered, Timestamp: d.Timestamp,
	})
	switch {
	case err == nil:
		c.failures.Store(0)
		if aerr := d.Ack(false); aerr != nil {
			c.log.Warn("amqp ack failed (the message will be redelivered)", "stage", "consume", "result", "error", "error", aerr)
		}
	case isInvalid(err):
		eventType := c.cfg.EventType
		var ie *InvalidError
		if errors.As(err, &ie) && ie.EventType != "" {
			eventType = ie.EventType
		}
		if c.cfg.Metrics != nil {
			c.cfg.Metrics.EventsConsumed.WithLabelValues(eventType, c.cfg.Queue, telemetry.ResultError).Inc()
		}
		c.log.Warn("invalid message rejected", "stage", "consume", "result", "error", "messageId", d.MessageId, "error", err)
		if nerr := d.Nack(false, false); nerr != nil {
			c.log.Warn("amqp nack failed", "stage", "consume", "result", "error", "error", nerr)
		}
	default:
		n := c.failures.Add(1)
		wait := Backoff(c.cfg.BackoffMin, c.cfg.BackoffMax, int(n))
		c.log.Warn("transient error, requeueing", "stage", "consume", "result", "error", "messageId", d.MessageId,
			"redelivered", d.Redelivered, "backoff", wait.String(), "error", err)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
		if nerr := d.Nack(false, true); nerr != nil {
			c.log.Warn("amqp nack failed", "stage", "consume", "result", "error", "error", nerr)
		}
	}
}

func isInvalid(err error) bool {
	var ie *InvalidError
	return errors.As(err, &ie)
}

// Backoff devuelve la espera tras n fallos consecutivos: min * 2^(n-1), acotada por max.
func Backoff(minD, maxD time.Duration, n int) time.Duration {
	d := minD
	for i := 1; i < n && d < maxD; i++ {
		d *= 2
	}
	return min(d, maxD)
}

// dropConnection cierra la conexión actual sin parar el Consumer (tests de reconexión).
func (c *Consumer) dropConnection() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}
