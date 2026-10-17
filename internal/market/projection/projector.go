// Package projection es el lado de lectura (CQRS) del mercado: market-projector consume
// energy-positions y kwh-prices y mantiene la vista Redis `market-view` que lee el
// matching-engine. Nunca toca positions-db: los eventos llevan el estado completo.
//
// Este paquete y su binario no pueden depender de Postgres (ver deps_test.go).
package projection

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

// Servicio, colas y failpoint.
const (
	ServiceName     = "market-projector"
	QueuePositions  = "energy-positions"
	QueuePrices     = "kwh-prices"
	FPDropPosition  = "drop_position"
	positionKeyPref = "pos:"
	priceKeyPref    = "price:"
)

// Viewer es la parte de view.Store que usa el proyector.
type Viewer interface {
	Upsert(ctx context.Context, key string, version int64, fields map[string]any, ttl time.Duration) (view.Result, error)
}

var zonePattern = regexp.MustCompile(`^z-[1-9][0-9]{0,3}$`)

// Projector aplica eventos a la vista.
type Projector struct {
	Region      string
	View        Viewer
	Svc         *telemetry.Service
	PositionTTL time.Duration
	PriceTTL    time.Duration
	Failpoints  *failpoint.Registry
	// Dropped cuenta los eventos descartados por un failpoint.
	Dropped *prometheus.CounterVec
	Clock   func() time.Time
}

// NewDroppedCounter crea failpoint_dropped_total{event_type} y lo registra en reg.
func NewDroppedCounter(reg prometheus.Registerer) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "failpoint_dropped_total", Help: "Eventos descartados (con ack) por un failpoint."}, []string{"event_type"})
	reg.MustRegister(c)
	return c
}

func (p *Projector) now() time.Time {
	if p.Clock != nil {
		return p.Clock()
	}
	return time.Now()
}

// PositionKey es la key de una posición: pos:<zona>:<medidor>.
func PositionKey(zone, meterID string) string { return positionKeyPref + zone + ":" + meterID }

// PriceKey es la key del precio de una región: price:<región>.
func PriceKey(region string) string { return priceKeyPref + region }

// HandlePosition es el amqpx.Handler de energy-positions.
func (p *Projector) HandlePosition(ctx context.Context, d amqpx.Delivery) error {
	start := p.now()
	env, err := events.Unmarshal(d.Body)
	if err != nil {
		return amqpx.InvalidAs(events.TypePositionDeclared, err)
	}
	pos, err := p.decodePosition(env)
	if err != nil {
		return amqpx.InvalidAs(events.TypePositionDeclared, err)
	}
	telemetry.LogStage(p.Svc.Logger, telemetry.StageConsume, env, telemetry.ResultOK, "meterId", pos.MeterID, "version", env.Version)

	if p.Failpoints != nil && p.Failpoints.Hit(ctx, FPDropPosition) {
		p.Dropped.WithLabelValues(env.Type).Inc()
		telemetry.LogStage(p.Svc.Logger, telemetry.StageApply, env, "dropped", "failpoint", FPDropPosition, "meterId", pos.MeterID)
		return nil // ack y descarta: simula la pérdida de un evento (E7)
	}

	now := p.now()
	fields := map[string]any{
		"meterId":     pos.MeterID,
		"side":        string(pos.Side),
		"kwh":         strconv.FormatFloat(pos.KWh, 'f', -1, 64),
		"validUntil":  pos.ValidUntil.UTC().Format(time.RFC3339Nano),
		"zone":        pos.Zone,
		"region":      env.Region,
		"eventId":     env.EventID,
		"occurredAt":  env.OccurredAt.UTC().Format(time.RFC3339Nano),
		"projectedAt": now.UTC().Format(time.RFC3339Nano),
	}
	if !env.CommittedAt.IsZero() {
		fields["committedAt"] = env.CommittedAt.UTC().Format(time.RFC3339Nano)
	}
	return p.apply(ctx, env, QueuePositions, PositionKey(pos.Zone, pos.MeterID), fields, p.PositionTTL, start)
}

func (p *Projector) decodePosition(env events.Envelope) (events.PositionDeclared, error) {
	var zero events.PositionDeclared
	if env.Type != events.TypePositionDeclared {
		return zero, fmt.Errorf("unexpected event type %q in energy-positions", env.Type)
	}
	if env.Region != p.Region {
		return zero, fmt.Errorf("event from region %q reached the projector of %q", env.Region, p.Region)
	}
	pos, err := events.DecodePayload[events.PositionDeclared](env)
	if err != nil {
		return zero, err
	}
	switch {
	case pos.Validate() != nil:
		return zero, pos.Validate()
	case !zonePattern.MatchString(pos.Zone):
		return zero, fmt.Errorf("invalid zone %q", pos.Zone)
	case pos.MeterID != env.AggregateID:
		return zero, fmt.Errorf("meterId %q differs from aggregateId %q", pos.MeterID, env.AggregateID)
	case env.Version < 1:
		return zero, fmt.Errorf("invalid version %d", env.Version)
	}
	return pos, nil
}

// HandlePrice es el amqpx.Handler de kwh-prices.
func (p *Projector) HandlePrice(ctx context.Context, d amqpx.Delivery) error {
	start := p.now()
	env, err := events.Unmarshal(d.Body)
	if err != nil {
		return amqpx.InvalidAs(events.TypePriceUpdated, err)
	}
	price, err := p.decodePrice(env)
	if err != nil {
		return amqpx.InvalidAs(events.TypePriceUpdated, err)
	}
	telemetry.LogStage(p.Svc.Logger, telemetry.StageConsume, env, telemetry.ResultOK, "region", price.Region, "version", env.Version)
	fields := map[string]any{
		"pricePerKwh": strconv.FormatFloat(price.PricePerKWh, 'f', -1, 64),
		"validUntil":  price.ValidUntil.UTC().Format(time.RFC3339Nano),
		"eventId":     env.EventID,
		"occurredAt":  env.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	return p.apply(ctx, env, QueuePrices, PriceKey(price.Region), fields, p.PriceTTL, start)
}

func (p *Projector) decodePrice(env events.Envelope) (events.PriceUpdated, error) {
	var zero events.PriceUpdated
	if env.Type != events.TypePriceUpdated {
		return zero, fmt.Errorf("unexpected event type %q in kwh-prices", env.Type)
	}
	price, err := events.DecodePayload[events.PriceUpdated](env)
	if err != nil {
		return zero, err
	}
	switch {
	case price.Region != p.Region || env.Region != p.Region:
		return zero, fmt.Errorf("price for region %q reached the projector of %q", price.Region, p.Region)
	case price.PricePerKWh <= 0:
		return zero, errors.New("pricePerKwh must be > 0")
	case env.Version < 1:
		return zero, fmt.Errorf("invalid version %d", env.Version)
	}
	return price, nil
}

// apply escribe en la vista (versión = Envelope.Version) y registra el resultado.
// Un error de Redis es transitorio: el mensaje vuelve a la cola.
func (p *Projector) apply(ctx context.Context, env events.Envelope, channel, key string, fields map[string]any, ttl time.Duration, start time.Time) error {
	res, err := p.View.Upsert(ctx, key, env.Version, fields, ttl)
	if err != nil {
		telemetry.LogStage(p.Svc.Logger, telemetry.StageApply, env, telemetry.ResultError, "key", key, "error", err)
		return err
	}
	result := telemetry.ResultOK
	if res == view.Stale {
		result = telemetry.ResultStale
	}
	now := p.now()
	telemetry.LogStage(p.Svc.Logger, telemetry.StageApply, env, result, "key", key, "version", env.Version)
	p.Svc.Consumed(env, channel, result)
	p.Svc.Metrics.ProcessingSeconds.WithLabelValues(env.Type).Observe(now.Sub(start).Seconds())
	if res == view.Applied {
		p.Svc.Metrics.ObserveLag(env, now) // event_e2e_lag_seconds{stage=committed}: commit en la base -> vista
	}
	return nil
}
