package amqpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// Errores de Send. Todos significan "el broker no tiene el mensaje".
var (
	// ErrUnroutable: el exchange no encaminó el mensaje a ninguna cola (mandatory).
	ErrUnroutable = errors.New("amqpx: message returned as unroutable")
	// ErrNacked: el broker respondió nack o el canal se cerró antes del confirm.
	ErrNacked = errors.New("amqpx: publish not confirmed by the broker")
)

const (
	defaultConfirmTimeout = 10 * time.Second
	defaultMaxInFlight    = 256
	// returnBuffer debe ser mayor que MaxInFlight: el dispatcher de amqp091 se bloquea si
	// se llena el canal de returns, y cada Send los drena solo tras su confirm.
	returnBuffer = 1024
)

// Outgoing es un mensaje listo para publicar.
type Outgoing struct {
	Exchange   string
	RoutingKey string
	Body       []byte
	MessageID  string
	// Type es la propiedad AMQP "type" (aquí, el tipo del evento o del formato).
	Type      string
	Timestamp time.Time
	Headers   map[string]any
}

// EnvelopeOutgoing arma el mensaje de un evento hacia el canal topic: exchange
// "<topic>.x", routing key "<topic>" (el binding de la cola usa su nombre) y headers
// traceparent, eventId y type. body es el sobre ya serializado.
func EnvelopeOutgoing(topic string, env events.Envelope, body []byte) Outgoing {
	h := map[string]any{"eventId": env.EventID, "type": env.Type}
	if env.Traceparent != "" {
		h["traceparent"] = env.Traceparent
	}
	return Outgoing{
		Exchange:   topic + ".x",
		RoutingKey: topic,
		Body:       body,
		MessageID:  env.EventID,
		Type:       env.Type,
		Timestamp:  time.Now(),
		Headers:    h,
	}
}

// PublisherOptions son los ajustes opcionales del Publisher.
type PublisherOptions struct {
	// ConfirmTimeout acota la espera del confirm (por defecto 10 s).
	ConfirmTimeout time.Duration
	// MaxInFlight acota los Send concurrentes (por defecto 256).
	MaxInFlight int
	Logger      *slog.Logger
}

// Publisher publica con publisher confirms y mandatory sobre un solo canal; Send es
// seguro para uso concurrente (varias goroutines esperan su confirm a la vez). Conecta
// perezosamente y reconecta en el siguiente Send tras un fallo de canal o de conexión.
type Publisher struct {
	cfg  Config
	opts PublisherOptions
	log  *slog.Logger

	mu      sync.Mutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns chan amqp.Return
	opened  bool // ya hubo un canal: el siguiente es una reconexión
	closed  bool

	retMu    sync.Mutex
	returned map[string]amqp.Return // por CorrelationId

	sem        chan struct{}
	seq        atomic.Uint64
	reconnects atomic.Int64
}

// NewPublisher crea el Publisher; no abre ninguna conexión hasta el primer Send o Ping.
func NewPublisher(cfg Config, opts PublisherOptions) *Publisher {
	if opts.ConfirmTimeout <= 0 {
		opts.ConfirmTimeout = defaultConfirmTimeout
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = defaultMaxInFlight
	}
	opts.MaxInFlight = min(opts.MaxInFlight, returnBuffer/2)
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Publisher{cfg: cfg, opts: opts, log: log, returned: map[string]amqp.Return{}, sem: make(chan struct{}, opts.MaxInFlight)}
}

// Reconnects cuenta las veces que hubo que abrir un canal o una conexión nuevos tras el primero.
func (p *Publisher) Reconnects() int64 { return p.reconnects.Load() }

// Ping asegura que hay conexión y canal con confirms (sirve de readiness check).
func (p *Publisher) Ping(ctx context.Context) error {
	_, _, err := p.channel(ctx)
	return err
}

// Send publica o, si no puede, devuelve un error. Vuelve solo cuando el broker confirmó
// (ack): un error significa "no confirmado" y el llamador debe reintentar (al-menos-una-vez).
func (p *Publisher) Send(ctx context.Context, o Outgoing) error {
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	ch, returns, err := p.channel(ctx)
	if err != nil {
		return err
	}
	corr := strconv.FormatUint(p.seq.Add(1), 10)
	hdr := make(amqp.Table, len(o.Headers))
	for k, v := range o.Headers {
		hdr[k] = v
	}
	ts := o.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, o.Exchange, o.RoutingKey, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     o.MessageID,
		CorrelationId: corr,
		Type:          o.Type,
		Timestamp:     ts,
		Headers:       hdr,
		Body:          o.Body,
	})
	if err != nil {
		p.invalidate(ch)
		return fmt.Errorf("amqpx: publish to %s: %w", o.Exchange, err)
	}

	wctx, cancel := context.WithTimeout(ctx, p.opts.ConfirmTimeout)
	defer cancel()
	acked, err := dc.WaitContext(wctx)
	if err != nil {
		if ctx.Err() == nil { // timeout propio: el canal no responde
			p.invalidate(ch)
		}
		return fmt.Errorf("amqpx: confirm from %s: %w", o.Exchange, err)
	}
	// El broker envía basic.return antes del ack; ya está en el buffer de returns.
	p.collectReturns(returns)
	if r, ok := p.takeReturn(corr); ok {
		return fmt.Errorf("%w: exchange %q key %q (%d %s)", ErrUnroutable, o.Exchange, o.RoutingKey, r.ReplyCode, r.ReplyText)
	}
	if !acked {
		if ch.IsClosed() {
			p.invalidate(ch)
		}
		return fmt.Errorf("%w: exchange %q", ErrNacked, o.Exchange)
	}
	return nil
}

func (p *Publisher) collectReturns(returns <-chan amqp.Return) {
	for {
		select {
		case r := <-returns:
			p.retMu.Lock()
			p.returned[r.CorrelationId] = r
			p.retMu.Unlock()
		default:
			return
		}
	}
}

func (p *Publisher) takeReturn(corr string) (amqp.Return, bool) {
	p.retMu.Lock()
	defer p.retMu.Unlock()
	r, ok := p.returned[corr]
	delete(p.returned, corr)
	return r, ok
}

// channel devuelve el canal vigente o abre conexión y canal nuevos.
func (p *Publisher) channel(ctx context.Context) (*amqp.Channel, <-chan amqp.Return, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, errors.New("amqpx: publisher closed")
	}
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, p.returns, nil
	}
	if p.conn == nil || p.conn.IsClosed() {
		conn, err := p.cfg.dial()
		if err != nil {
			return nil, nil, err
		}
		p.conn = conn
	}
	ch, err := p.conn.Channel()
	if err != nil {
		_ = p.conn.Close()
		p.conn = nil
		return nil, nil, fmt.Errorf("amqpx: open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = p.conn.Close()
		p.conn, p.ch = nil, nil
		return nil, nil, fmt.Errorf("amqpx: enable confirms: %w", err)
	}
	if p.opened {
		p.reconnects.Add(1)
		p.log.Warn("amqp publisher reconnected", "stage", "publish", "vhost", p.cfg.Vhost)
	}
	p.opened = true
	p.ch = ch
	p.returns = ch.NotifyReturn(make(chan amqp.Return, returnBuffer))
	p.retMu.Lock()
	p.returned = map[string]amqp.Return{}
	p.retMu.Unlock()
	return ch, p.returns, nil
}

// invalidate descarta ch para que el siguiente Send abra uno nuevo (y conexión, si cayó).
func (p *Publisher) invalidate(ch *amqp.Channel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == ch {
		p.ch = nil
		go func() { _ = ch.Close() }()
	}
	if p.conn != nil && p.conn.IsClosed() {
		p.conn = nil
	}
}

// Close cierra el canal y la conexión; los Send posteriores fallan.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	var err error
	if p.conn != nil && !p.conn.IsClosed() {
		err = p.conn.Close()
	}
	p.conn, p.ch = nil, nil
	return err
}
