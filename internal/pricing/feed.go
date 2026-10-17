// Package pricing es el price-feed-simulator: publica PriceUpdated de la región a
// intervalos aleatorios con un precio que sigue un paseo aleatorio acotado.
//
// No hay base de datos ni outbox: el precio es estado efímero y regenerable. No existe
// una transacción de negocio con la que publicar de forma atómica y perder un evento no
// deja ningún daño: el siguiente (en <= PRICE_MAX_INTERVAL_S) trae el estado completo
// y una versión mayor. Por eso publica directo y reintenta sin persistir nada.
package pricing

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/w3c"
)

// Servicio y canal.
const (
	ServiceName = "price-feed-simulator"
	Topic       = "kwh-prices"
)

// boundSteps: el precio se mantiene en [base - boundSteps*jitter, base + boundSteps*jitter].
const boundSteps = 5

// Walk es un paseo aleatorio acotado alrededor de un precio base.
type Walk struct {
	rng    *rand.Rand
	price  float64
	jitter float64
	lo, hi float64
}

// NewWalk crea el paseo: parte de base, cada paso suma un valor uniforme en
// [-jitter, +jitter] y el resultado se acota a base ± 5*jitter (y nunca baja de 0,0001).
func NewWalk(base, jitter float64, seed int64) *Walk {
	return &Walk{
		rng:    rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15)),
		price:  base,
		jitter: jitter,
		lo:     math.Max(base-boundSteps*jitter, 0.0001),
		hi:     base + boundSteps*jitter,
	}
}

// Next avanza un paso y devuelve el precio (4 decimales).
func (w *Walk) Next() float64 {
	w.price += (w.rng.Float64()*2 - 1) * w.jitter
	w.price = math.Min(math.Max(w.price, w.lo), w.hi)
	return math.Round(w.price*10000) / 10000
}

// Sender publica un mensaje y vuelve tras el confirm del broker (*amqpx.Publisher).
type Sender interface {
	Send(ctx context.Context, o amqpx.Outgoing) error
}

// Feed publica precios.
type Feed struct {
	Region      string
	Walk        *Walk
	Sender      Sender
	Svc         *telemetry.Service
	MinInterval time.Duration
	MaxInterval time.Duration
	Clock       func() time.Time
	// rng sortea los intervalos.
	rng     *rand.Rand
	lastVer int64
}

// NewFeed crea el Feed; seed fija la secuencia de intervalos (y la del Walk, que se crea aparte).
func NewFeed(region string, walk *Walk, sender Sender, svc *telemetry.Service, minI, maxI time.Duration, seed int64) *Feed {
	return &Feed{
		Region: region, Walk: walk, Sender: sender, Svc: svc, MinInterval: minI, MaxInterval: maxI,
		rng: rand.New(rand.NewPCG(uint64(seed)+1, uint64(seed)^0xd1b54a32d192ed03)),
	}
}

func (f *Feed) now() time.Time {
	if f.Clock != nil {
		return f.Clock()
	}
	return time.Now()
}

// Interval sortea la espera hasta el siguiente precio, uniforme en [Min, Max].
func (f *Feed) Interval() time.Duration {
	span := f.MaxInterval - f.MinInterval
	if span <= 0 {
		return f.MinInterval
	}
	return f.MinInterval + time.Duration(f.rng.Float64()*float64(span))
}

// NextEvent arma el siguiente PriceUpdated: version = unix ms monótona (estrictamente
// creciente aunque el reloj no avance), validUntil = ahora + 2 × el intervalo máximo.
func (f *Feed) NextEvent() (events.Envelope, error) {
	now := f.now().UTC()
	ver := max(now.UnixMilli(), f.lastVer+1)
	env, err := events.NewPriceUpdated(f.Region, ver, now, events.PriceUpdated{
		Region:      f.Region,
		PricePerKWh: f.Walk.Next(),
		ValidUntil:  now.Add(2 * f.MaxInterval),
	})
	if err != nil {
		return events.Envelope{}, err
	}
	f.lastVer = ver
	env.EmittedAt = now
	return env.WithTraceparent(w3c.NewTraceparent()), nil
}

// PublishOnce arma y publica un precio. Devuelve el sobre publicado.
func (f *Feed) PublishOnce(ctx context.Context) (events.Envelope, error) {
	env, err := f.NextEvent()
	if err != nil {
		return env, err
	}
	body, err := events.Marshal(env)
	if err != nil {
		return env, err
	}
	if err := f.Sender.Send(ctx, amqpx.EnvelopeOutgoing(Topic, env, body)); err != nil {
		telemetry.LogStage(f.Svc.Logger, telemetry.StagePublish, env, telemetry.ResultError, "topic", Topic, "error", err)
		return env, err
	}
	f.Svc.Published(env, Topic)
	p, _ := events.DecodePayload[events.PriceUpdated](env)
	telemetry.LogStage(f.Svc.Logger, telemetry.StagePublish, env, telemetry.ResultOK, "topic", Topic,
		"pricePerKwh", p.PricePerKWh, "version", env.Version)
	return env, nil
}

// retryAfterFailure es la espera tras un publish fallido (el precio siguiente no espera el intervalo completo).
const retryAfterFailure = 2 * time.Second

// Run publica un precio al arrancar y luego uno cada Interval() hasta que ctx se cancele.
// Un fallo no detiene el bucle: se reintenta con un precio nuevo (el estado es completo).
func (f *Feed) Run(ctx context.Context) error {
	for {
		wait := f.Interval()
		if _, err := f.PublishOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			wait = min(wait, retryAfterFailure)
			f.Svc.Logger.Warn("price publish failed, will retry with a fresh price", "stage", "publish", "result", "error",
				"error", err, "retryIn", wait.String(), slog.String("topic", Topic))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}
