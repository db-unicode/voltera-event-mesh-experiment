package natsx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
)

// Delivery es un mensaje entregado al handler.
type Delivery struct {
	Subject      string
	Header       nats.Header
	Data         []byte
	NumDelivered uint64
}

// DeliveryHandler procesa un mensaje. nil ⇒ ack explícito; error ⇒ Nak con retardo
// (el servidor lo reentrega).
type DeliveryHandler func(ctx context.Context, d Delivery) error

// PullConfig configura el PullConsumer.
type PullConfig struct {
	Stream  string
	Durable string
	Batch   int           // mensajes por Fetch (por defecto 50)
	MaxWait time.Duration // espera del Fetch (por defecto 2 s)
	// NakDelay es el retardo de reentrega tras un error del handler (por defecto 2 s).
	NakDelay   time.Duration
	Logger     *slog.Logger
	Registerer prometheus.Registerer
}

// PullConsumer lee con un consumer durable que ya existe en el servidor (creado por
// mesh:up: el usuario de Facturación no puede crear consumers) y confirma cada
// mensaje solo después del handler.
type PullConsumer struct {
	cons    jetstream.Consumer
	cfg     PullConfig
	log     *slog.Logger
	handled *prometheus.CounterVec
}

// NewPullConsumer enlaza con el durable. Falla si no existe o no hay permisos.
func NewPullConsumer(ctx context.Context, c *Client, cfg PullConfig) (*PullConsumer, error) {
	if cfg.Stream == "" || cfg.Durable == "" {
		return nil, errors.New("natsx: stream and durable are required")
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 50
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 2 * time.Second
	}
	if cfg.NakDelay <= 0 {
		cfg.NakDelay = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cons, err := c.js.Consumer(ctx, cfg.Stream, cfg.Durable)
	if err != nil {
		return nil, fmt.Errorf("natsx: consumer %s/%s: %w", cfg.Stream, cfg.Durable, err)
	}
	p := &PullConsumer{cons: cons, cfg: cfg, log: cfg.Logger,
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nats_pull_handled_total", Help: "Mensajes del consumer durable por resultado.",
			ConstLabels: prometheus.Labels{"stream": cfg.Stream, "durable": cfg.Durable}}, []string{"result"})}
	p.handled.WithLabelValues("ok")
	p.handled.WithLabelValues("error")
	if cfg.Registerer != nil {
		cfg.Registerer.MustRegister(p.handled)
	}
	return p, nil
}

// Run hace Fetch en bucle hasta que ctx se cancele (devuelve nil).
func (p *PullConsumer) Run(ctx context.Context, h DeliveryHandler) error {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		batch, err := p.cons.Fetch(p.cfg.Batch, jetstream.FetchMaxWait(p.cfg.MaxWait))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			p.log.Warn("fetch failed", "stage", "consume", "result", "error", "error", err)
			if !sleep(ctx, backoff) {
				break
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 200 * time.Millisecond
		for msg := range batch.Messages() {
			p.handle(ctx, h, msg)
		}
		if err := batch.Error(); err != nil && ctx.Err() == nil && !errors.Is(err, nats.ErrTimeout) {
			p.log.Warn("fetch batch error", "stage", "consume", "result", "error", "error", err)
		}
	}
	return nil
}

func (p *PullConsumer) handle(ctx context.Context, h DeliveryHandler, msg jetstream.Msg) {
	var delivered uint64
	if md, err := msg.Metadata(); err == nil {
		delivered = md.NumDelivered
	}
	hctx := context.WithoutCancel(ctx)
	err := h(hctx, Delivery{Subject: msg.Subject(), Header: msg.Headers(), Data: msg.Data(), NumDelivered: delivered})
	if err != nil {
		p.handled.WithLabelValues("error").Inc()
		p.log.Warn("handler failed; nak", "stage", "consume", "result", "error", "subject", msg.Subject(), "error", err)
		_ = msg.NakWithDelay(p.cfg.NakDelay)
		return
	}
	p.handled.WithLabelValues("ok").Inc()
	actx, cancel := context.WithTimeout(hctx, 5*time.Second)
	defer cancel()
	if err := msg.DoubleAck(actx); err != nil {
		// sin confirmar el ack el servidor reentrega tras ack_wait: el handler es idempotente
		p.log.Warn("ack failed; the message will be redelivered", "stage", "commit", "result", "error",
			"subject", msg.Subject(), "error", err)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
