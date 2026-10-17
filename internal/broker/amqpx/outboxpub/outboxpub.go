// Package outboxpub adapta amqpx.Publisher a outbox.Publisher. Vive aparte porque
// internal/outbox importa pgx y los binarios que solo consumen (market-projector) no
// deben enlazarlo.
package outboxpub

import (
	"context"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
)

// Publisher publica los mensajes del relay de la outbox en RabbitMQ: exchange
// "<topic>.x", routing key "<topic>", persistente, mandatory, y vuelve tras el confirm.
type Publisher struct {
	*amqpx.Publisher
}

var _ outbox.Publisher = (*Publisher)(nil)

// New envuelve p.
func New(p *amqpx.Publisher) *Publisher { return &Publisher{Publisher: p} }

// Publish implementa outbox.Publisher. msg.Key (clave de partición) no se usa: la ruta
// en un exchange direct es el nombre de la cola.
func (p *Publisher) Publish(ctx context.Context, msg outbox.Message) error {
	env := msg.Envelope
	if msg.Traceparent != "" {
		env.Traceparent = msg.Traceparent
	}
	return p.Send(ctx, amqpx.EnvelopeOutgoing(msg.Topic, env, msg.Body))
}
