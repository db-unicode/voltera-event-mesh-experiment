// Package kafkax son los adaptadores de Kafka (franz-go) de los servicios: un
// Publisher para la outbox y un Consumer de grupo con commit manual tras el handler.
package kafkax

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
)

// Nombres de los headers que lleva cada mensaje.
const (
	HeaderTraceparent = "traceparent"
	HeaderEventID     = "eventId"
	HeaderType        = "type"
)

// PublisherConfig configura el Publisher.
type PublisherConfig struct {
	Brokers  []string
	ClientID string
	// DeliveryTimeout acota cuánto espera el ack del broker (por defecto 30 s).
	DeliveryTimeout time.Duration
	// Opts se añaden al final (tests, ajustes).
	Opts []kgo.Opt
}

// Publisher implementa outbox.Publisher sobre franz-go: acks=all, productor
// idempotente (el valor por defecto del cliente) y la clave del mensaje decide la
// partición. Publish vuelve solo tras el ack del broker.
type Publisher struct {
	cl *kgo.Client
}

var _ outbox.Publisher = (*Publisher)(nil)

// NewPublisher crea el cliente productor. No conecta hasta la primera publicación.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkax: no brokers")
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.ProducerBatchMaxBytes(1 << 20),
	}
	cl, err := kgo.NewClient(append(opts, cfg.Opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: new producer: %w", err)
	}
	return &Publisher{cl: cl}, nil
}

// ProducerRecord arma el registro Kafka de un mensaje de la outbox.
func ProducerRecord(msg outbox.Message) *kgo.Record {
	hdrs := []kgo.RecordHeader{
		{Key: HeaderEventID, Value: []byte(msg.EventID)},
		{Key: HeaderType, Value: []byte(msg.Type)},
	}
	if msg.Traceparent != "" {
		hdrs = append(hdrs, kgo.RecordHeader{Key: HeaderTraceparent, Value: []byte(msg.Traceparent)})
	}
	return &kgo.Record{Topic: msg.Topic, Key: []byte(msg.Key), Value: msg.Body, Headers: hdrs}
}

// Publish envía el mensaje y espera el ack de todas las réplicas in-sync.
func (p *Publisher) Publish(ctx context.Context, msg outbox.Message) error {
	if err := p.cl.ProduceSync(ctx, ProducerRecord(msg)).FirstErr(); err != nil {
		return fmt.Errorf("kafkax: produce %s to %s: %w", msg.EventID, msg.Topic, err)
	}
	return nil
}

// Ping comprueba que hay al menos un broker alcanzable (readyz).
func (p *Publisher) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

// Close cierra el cliente tras vaciar lo pendiente.
func (p *Publisher) Close() { p.cl.Close() }
