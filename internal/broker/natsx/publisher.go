package natsx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Razones de rechazo (etiqueta reason de mesh_publish_rejected_total).
const (
	ReasonNoAck      = "no_ack"     // sin PubAck: enlace caído, stream no disponible o timeout
	ReasonPermission = "permission" // el servidor denegó el subject
	ReasonOther      = "other"
)

// RejectedError es un publish sin PubAck. El mensaje no está confirmado: el llamador
// no debe avanzar (p. ej. no confirmar el offset de Kafka) y debe reintentar.
type RejectedError struct {
	Reason string
	Err    error
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("natsx: publish rejected (%s): %v", e.Reason, e.Err)
}
func (e *RejectedError) Unwrap() error { return e.Err }

// Msg es un mensaje a publicar.
type Msg struct {
	Subject string
	// MsgID va en el header Nats-Msg-Id: el stream descarta repetidos dentro de su
	// duplicate_window.
	MsgID  string
	Header map[string]string // p. ej. traceparent
	Data   []byte
}

// Publisher publica en JetStream y solo devuelve nil con el PubAck del stream.
type Publisher struct {
	c          *Client
	ackTimeout time.Duration
}

// NewPublisher crea el publicador; ackTimeout es la espera máxima del PubAck por
// intento (por defecto 5 s).
func NewPublisher(c *Client, ackTimeout time.Duration) *Publisher {
	if ackTimeout <= 0 {
		ackTimeout = 5 * time.Second
	}
	return &Publisher{c: c, ackTimeout: ackTimeout}
}

// Publish envía el mensaje con PublishMsg y espera el PubAck. Un Duplicate=true (el
// stream ya tenía ese Nats-Msg-Id) también cuenta como confirmado. Sin reintentos
// internos: el llamador decide el backoff.
func (p *Publisher) Publish(ctx context.Context, m Msg) (*jetstream.PubAck, error) {
	if m.Subject == "" || m.MsgID == "" {
		return nil, errors.New("natsx: subject and msgID are required")
	}
	hdr := nats.Header{jetstream.MsgIDHeader: []string{m.MsgID}}
	for k, v := range m.Header {
		if v != "" {
			hdr.Set(k, v)
		}
	}
	before := p.c.Violations()
	actx, cancel := context.WithTimeout(ctx, p.ackTimeout)
	defer cancel()
	ack, err := p.c.js.PublishMsg(actx, &nats.Msg{Subject: m.Subject, Header: hdr, Data: m.Data}, jetstream.WithRetryAttempts(0))
	if err == nil {
		return ack, nil
	}
	return nil, &RejectedError{Reason: p.reason(ctx, before, err), Err: err}
}

// reason clasifica el fallo. El servidor avisa de las violaciones de forma
// asíncrona: se espera un instante a ver si llegó una.
func (p *Publisher) reason(ctx context.Context, violationsBefore int, err error) string {
	deadline := time.Now().Add(100 * time.Millisecond)
	for {
		if p.c.Violations() > violationsBefore {
			return ReasonPermission
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrDisconnected) {
		return ReasonNoAck
	}
	return ReasonOther
}
