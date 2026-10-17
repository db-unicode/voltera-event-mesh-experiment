// Package metering es el contexto de medición: el declaration-gateway recibe las
// declaraciones de los medidores (cada fabricante con su formato), las traduce con la ACL
// y guarda la posición en positions-db junto con el evento PositionDeclared (outbox).
package metering

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// Service y canales del gateway.
const (
	ServiceName = "declaration-gateway"
	// QueueDeclarations es la cola de entrada (quorum, vhost de la región).
	QueueDeclarations = "meter-declarations"
	// EventTypeDeclaration etiqueta las métricas de lo consumido, que aún no es un evento de dominio.
	EventTypeDeclaration = "MeterDeclaration"
	// FPBeforeAck: failpoint tras el commit y antes del ack (el mensaje se reentrega).
	FPBeforeAck = "before_ack"
)

// Store guarda una posición y su evento en una sola transacción.
type Store interface {
	// Apply devuelve applied=false (sin escribir nada) si la versión no es mayor que la guardada.
	Apply(ctx context.Context, env events.Envelope, p events.PositionDeclared) (applied bool, err error)
}

// Gateway procesa los mensajes de meter-declarations.
type Gateway struct {
	Region string
	ACL    *acl.ACL
	Store  Store
	Svc    *telemetry.Service
	// Domain cuenta declarations_processed_total.
	Domain *telemetry.DomainMetrics
	// Rejected cuenta declarations_rejected_total{vendor,reason}.
	Rejected *prometheus.CounterVec
	// Delay simula el coste de validar y traducir (PROCESSING_DELAY_MS): hace visible el
	// escalado con carga modesta.
	Delay time.Duration
	// Wake despierta al relay tras un commit (puede ser nil).
	Wake       func()
	Failpoints *failpoint.Registry
	Clock      func() time.Time
}

// NewRejectedCounter crea declarations_rejected_total y lo registra en reg.
func NewRejectedCounter(reg prometheus.Registerer) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "declarations_rejected_total", Help: "Declaraciones rechazadas por la ACL (sin reintento)."}, []string{"vendor", "reason"})
	reg.MustRegister(c)
	return c
}

func (g *Gateway) now() time.Time {
	if g.Clock != nil {
		return g.Clock()
	}
	return time.Now()
}

// Handle es el amqpx.Handler del gateway. nil ⇒ ack (aplicado o duplicado/viejo);
// amqpx.Invalid ⇒ rechazo definitivo; cualquier otro error ⇒ reintento.
func (g *Gateway) Handle(ctx context.Context, d amqpx.Delivery) error {
	start := g.now()
	log := g.Svc.Logger
	vendor := d.Header("x-vendor")
	traceparent := d.Header("traceparent")

	if g.Delay > 0 {
		select {
		case <-time.After(g.Delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	decl, err := g.ACL.Translate(vendor, d.Body)
	if err != nil {
		reason, label := acl.ReasonInvalid, "unknown"
		var re *acl.RejectError
		if errors.As(err, &re) {
			reason = re.Reason
			if g.ACL.Known(vendor) {
				label = vendor // solo vendors conocidos como etiqueta: sin cardinalidad abierta
			}
		}
		g.Rejected.WithLabelValues(label, reason).Inc()
		log.Warn("declaration rejected", "stage", "consume", "result", "error", "vendor", vendor, "reason", reason,
			"eventId", d.Header("eventId"), "traceId", events.TraceID(traceparent), "error", err)
		return amqpx.InvalidAs(EventTypeDeclaration, err)
	}

	env, err := events.NewPositionDeclared(g.Region, decl.Version, decl.OccurredAt, decl.Position)
	if err != nil {
		return amqpx.InvalidAs(EventTypeDeclaration, err)
	}
	// El eventId de la declaración (lo fija el medidor) identifica también al evento de
	// dominio: permite seguir el mismo id de punta a punta. Sin él, el sobre trae uno nuevo.
	if id := d.Header("eventId"); id != "" {
		if _, perr := uuid.Parse(id); perr == nil {
			env.EventID = id
		}
	}
	env = env.WithTraceparent(traceparent)
	telemetry.LogStage(log, telemetry.StageConsume, env, telemetry.ResultOK, "vendor", vendor, "meterId", decl.Position.MeterID, "version", env.Version)

	applied, err := g.Store.Apply(ctx, env, decl.Position)
	if err != nil {
		telemetry.LogStage(log, telemetry.StageCommit, env, telemetry.ResultError, "error", err)
		return err // transitorio: la base no respondió; el mensaje vuelve a la cola
	}
	result := telemetry.ResultStale
	if applied {
		result = telemetry.ResultOK
		if g.Wake != nil {
			g.Wake()
		}
	}
	telemetry.LogStage(log, telemetry.StageCommit, env, result, "meterId", decl.Position.MeterID, "version", env.Version)
	g.Svc.Consumed(env, QueueDeclarations, result)
	g.Domain.DeclarationsProcessed.Inc()
	now := g.now()
	g.Svc.Metrics.ProcessingSeconds.WithLabelValues(env.Type).Observe(now.Sub(start).Seconds())
	if applied {
		g.Svc.Metrics.ObserveLag(env, now)
	}
	if g.Failpoints != nil {
		g.Failpoints.Hit(ctx, FPBeforeAck)
	}
	return nil
}
