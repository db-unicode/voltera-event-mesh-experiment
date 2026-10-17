package telemetry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// LagBuckets son los buckets de los histogramas de lag (sección 4.2).
var LagBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120}

// Metrics son las métricas comunes de eventos (tabla 4.2). Todas llevan la etiqueta
// constante service. Las métricas outbox_* y view_writes_total las registran los
// paquetes outbox y view en el mismo registro.
type Metrics struct {
	EventsPublished   *prometheus.CounterVec // event_type, channel
	EventsConsumed    *prometheus.CounterVec // event_type, channel, result=ok|duplicate|stale|error
	DuplicatesDropped *prometheus.CounterVec // event_type
	StaleDropped      *prometheus.CounterVec // event_type
	ProcessingSeconds *prometheus.HistogramVec
	E2ELagSeconds     *prometheus.HistogramVec // event_type, stage=occurred|committed|emitted
}

// NewMetrics crea y registra las métricas comunes en reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_published_total", Help: "Eventos publicados al canal."}, []string{"event_type", "channel"}),
		EventsConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_consumed_total", Help: "Eventos consumidos por resultado."}, []string{"event_type", "channel", "result"}),
		DuplicatesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_duplicates_dropped_total", Help: "Duplicados descartados por idempotencia."}, []string{"event_type"}),
		StaleDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_stale_dropped_total", Help: "Eventos con versión vieja descartados."}, []string{"event_type"}),
		ProcessingSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "event_processing_seconds", Help: "Tiempo de consumo hasta el ack.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}}, []string{"event_type"}),
		E2ELagSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "event_e2e_lag_seconds", Help: "now - timestamp del evento al terminar de aplicarlo.",
			Buckets: LagBuckets}, []string{"event_type", "stage"}),
	}
	reg.MustRegister(m.EventsPublished, m.EventsConsumed, m.DuplicatesDropped, m.StaleDropped, m.ProcessingSeconds, m.E2ELagSeconds)
	return m
}

// ObserveLag registra event_e2e_lag_seconds para los tres sellos del evento que
// existan (occurred, committed, emitted). Llamar al terminar de aplicar el evento.
func (m *Metrics) ObserveLag(env events.Envelope, now time.Time) {
	for stage, ts := range map[string]time.Time{
		"occurred":  env.OccurredAt,
		"committed": env.CommittedAt,
		"emitted":   env.EmittedAt,
	} {
		if !ts.IsZero() {
			m.E2ELagSeconds.WithLabelValues(env.Type, stage).Observe(max(now.Sub(ts).Seconds(), 0))
		}
	}
}

// DomainMetrics son las métricas de dominio de la tabla 4.2 que usan servicios concretos.
// Cada servicio las crea solo si las necesita (sin observaciones no exportan series, salvo
// las que no llevan etiquetas).
type DomainMetrics struct {
	DeclarationsProcessed prometheus.Counter     // gateway
	MatchesCreated        *prometheus.CounterVec // region
	MatchWindowSeconds    prometheus.Histogram
	TransferCommands      *prometheus.CounterVec // result=sent|failed|duplicate
	MeshPublished         *prometheus.CounterVec // region
	MeshPublishRejected   *prometheus.CounterVec // region, reason
	BillingReceived       *prometheus.CounterVec // region
	BillingDuplicates     *prometheus.CounterVec // region
}

// NewDomainMetrics crea y registra las métricas de dominio en reg.
func NewDomainMetrics(reg prometheus.Registerer) *DomainMetrics {
	d := &DomainMetrics{
		DeclarationsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "declarations_processed_total", Help: "Declaraciones procesadas por el gateway."}),
		MatchesCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "matches_created_total", Help: "Matches creados."}, []string{"region"}),
		MatchWindowSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "match_window_seconds", Help: "Duración de la ventana de matching.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10}}),
		TransferCommands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "transfer_commands_total", Help: "Comandos de transferencia."}, []string{"result"}),
		MeshPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mesh_published_total", Help: "Eventos publicados al event mesh."}, []string{"region"}),
		MeshPublishRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mesh_publish_rejected_total", Help: "Publicaciones rechazadas por el mesh."}, []string{"region", "reason"}),
		BillingReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "billing_received_total", Help: "Matches recibidos por facturación."}, []string{"region"}),
		BillingDuplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "billing_duplicates_total", Help: "Duplicados detectados en facturación."}, []string{"region"}),
	}
	reg.MustRegister(d.DeclarationsProcessed, d.MatchesCreated, d.MatchWindowSeconds, d.TransferCommands,
		d.MeshPublished, d.MeshPublishRejected, d.BillingReceived, d.BillingDuplicates)
	return d
}
