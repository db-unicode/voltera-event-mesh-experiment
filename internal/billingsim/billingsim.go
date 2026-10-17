// Package billingsim es billing-sim (voltera-global): Facturación simulada. Lee el
// consumer durable "billing" del stream BILLING_MATCHED con ack explícito y registra cada
// match de forma idempotente por matchId, para que los experimentos comprueben que un
// match repetido (reenvío tras la ventana de dedupe del stream, redelivery) no se factura
// dos veces. El estado es en memoria; el durable conserva su posición en el servidor.
package billingsim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/boot"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/natsx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

const serviceName = "billing-sim"

// RegionStats son los contadores de una región.
type RegionStats struct {
	Total      int `json:"total"`
	Distinct   int `json:"distinct"`
	Duplicates int `json:"duplicates"`
}

// State es la respuesta de GET /state.
type State struct {
	Total      int                    `json:"total"`
	Distinct   int                    `json:"distinct"`
	Duplicates int                    `json:"duplicates"`
	ByRegion   map[string]RegionStats `json:"byRegion"`
	// ByMatchID (solo con ?detail=1) son las entregas por matchId y su región.
	ByMatchID map[string]MatchEntry `json:"byMatchId,omitempty"`
}

// MatchEntry es lo facturado de un matchId. Los sellos y latencias son los de la primera
// entrega: un duplicado no los mueve.
type MatchEntry struct {
	Region     string    `json:"region"`
	Count      int       `json:"count"`
	ReceivedAt time.Time `json:"receivedAt,omitzero"`
	EmittedAt  time.Time `json:"emittedAt,omitzero"`
	OccurredAt time.Time `json:"occurredAt,omitzero"`
	// EmitLagMs = receivedAt - emittedAt (Kafka + mesh-bridge + NATS + leafnode); 0 si el sobre no trae emittedAt.
	EmitLagMs float64 `json:"emitLagMs,omitempty"`
	// E2ELagMs = receivedAt - occurredAt (desde que el motor decide el match).
	E2ELagMs float64 `json:"e2eLagMs,omitempty"`
}

// Times son los sellos de una entrega.
type Times struct {
	ReceivedAt, EmittedAt, OccurredAt time.Time
}

// Ledger es el libro de facturación idempotente por matchId.
type Ledger struct {
	mu      sync.Mutex
	byMatch map[string]*MatchEntry
	region  map[string]*RegionStats
	total   int
}

// NewLedger crea un libro vacío.
func NewLedger() *Ledger {
	return &Ledger{byMatch: map[string]*MatchEntry{}, region: map[string]*RegionStats{}}
}

// Record registra una entrega sin sellos y devuelve true si matchId ya estaba facturado.
func (l *Ledger) Record(region, matchID string) (duplicate bool) {
	return l.RecordAt(region, matchID, Times{})
}

// RecordAt registra una entrega con sus sellos (solo la primera los fija) y devuelve true si
// matchId ya estaba facturado.
func (l *Ledger) RecordAt(region, matchID string, t Times) (duplicate bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rs := l.region[region]
	if rs == nil {
		rs = &RegionStats{}
		l.region[region] = rs
	}
	l.total++
	rs.Total++
	e := l.byMatch[matchID]
	if e == nil {
		e = &MatchEntry{Region: region, ReceivedAt: t.ReceivedAt, EmittedAt: t.EmittedAt, OccurredAt: t.OccurredAt}
		if !t.ReceivedAt.IsZero() && !t.EmittedAt.IsZero() {
			e.EmitLagMs = float64(t.ReceivedAt.Sub(t.EmittedAt)) / float64(time.Millisecond)
		}
		if !t.ReceivedAt.IsZero() && !t.OccurredAt.IsZero() {
			e.E2ELagMs = float64(t.ReceivedAt.Sub(t.OccurredAt)) / float64(time.Millisecond)
		}
		l.byMatch[matchID] = e
	}
	e.Count++
	if e.Count > 1 {
		rs.Duplicates++
		return true
	}
	rs.Distinct++
	return false
}

// Snapshot devuelve el estado; detail añade las entregas por matchId.
func (l *Ledger) Snapshot(detail bool) State {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := State{Total: l.total, Distinct: len(l.byMatch), Duplicates: l.total - len(l.byMatch),
		ByRegion: make(map[string]RegionStats, len(l.region))}
	for r, rs := range l.region {
		st.ByRegion[r] = *rs
	}
	if detail {
		st.ByMatchID = make(map[string]MatchEntry, len(l.byMatch))
		for k, v := range l.byMatch {
			st.ByMatchID[k] = *v
		}
	}
	return st
}

// Biller procesa las entregas del stream.
type Biller struct {
	Ledger  *Ledger
	Svc     *telemetry.Service
	Metrics *telemetry.DomainMetrics
	Log     *slog.Logger
	Now     func() time.Time
}

// Handle implementa natsx.DeliveryHandler. Un mensaje ilegible se confirma y descarta
// (reentregarlo no lo arregla).
func (b *Biller) Handle(_ context.Context, d natsx.Delivery) error {
	env, err := events.Unmarshal(d.Data)
	if err != nil {
		b.Log.Error("poison message skipped", "stage", "received", "result", telemetry.ResultError, "subject", d.Subject, "error", err)
		return nil
	}
	if tp := d.Header.Get("traceparent"); tp != "" {
		env.Traceparent = tp
	}
	m, err := events.DecodePayload[events.EnergyMatched](env)
	if err != nil || m.MatchID == "" {
		b.Log.Error("poison message skipped", "stage", "received", "result", telemetry.ResultError, "subject", d.Subject, "error", err)
		return nil
	}
	region := regionOf(d.Subject, env.Region)

	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	received := now()
	dup := b.Ledger.RecordAt(region, m.MatchID, Times{ReceivedAt: received, EmittedAt: env.EmittedAt, OccurredAt: env.OccurredAt})
	b.Metrics.BillingReceived.WithLabelValues(region).Inc()
	result := telemetry.ResultOK
	if dup {
		result = telemetry.ResultDuplicate
		b.Metrics.BillingDuplicates.WithLabelValues(region).Inc()
	} else {
		b.Svc.Metrics.ObserveLag(env, received)
	}
	b.Svc.Consumed(env, d.Subject, result)
	telemetry.LogStage(b.Log, telemetry.StageReceived, env, result,
		"matchId", m.MatchID, "region", region, "kwh", m.KWh, "delivered", d.NumDelivered)
	return nil
}

// regionOf extrae <region> de energy.matched.<region>.<matchId> (o usa fallback).
func regionOf(subject, fallback string) string {
	parts := strings.Split(subject, ".")
	if len(parts) >= 4 && parts[2] != "" {
		return parts[2]
	}
	return fallback
}

// Handler devuelve el mux de la API (puerto http-api).
func Handler(l *Ledger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(l.Snapshot(r.URL.Query().Get("detail") == "1"))
	})
	return mux
}

// Config se lee de variables de entorno y se valida al arrancar.
type Config struct {
	NATS     natsx.Config
	Stream   string
	Durable  string
	HTTPAddr string
	OpsAddr  string
}

// LoadConfig lee la configuración. Vars: NATS_URL, NATS_USER, NATS_PASSWORD,
// STREAM (BILLING_MATCHED), DURABLE (billing), HTTP_ADDR (:8080), OPS_ADDR (:9090).
// REGION (global) solo etiqueta las métricas.
func LoadConfig(getenv func(string) string) (Config, error) {
	e := boot.NewEnv(getenv)
	c := Config{
		NATS:     natsx.Config{URL: e.Required("NATS_URL"), User: e.Required("NATS_USER"), Password: e.Required("NATS_PASSWORD"), Name: serviceName},
		Stream:   e.String("STREAM", "BILLING_MATCHED"),
		Durable:  e.String("DURABLE", "billing"),
		HTTPAddr: e.Addr("HTTP_ADDR", ":8080"),
		OpsAddr:  e.Addr("OPS_ADDR", ":9090"),
	}
	if err := e.Err(); err != nil {
		return c, err
	}
	if !strings.HasPrefix(c.NATS.URL, "nats://") && !strings.HasPrefix(c.NATS.URL, "tls://") {
		return c, errors.New("NATS_URL must start with nats:// or tls://")
	}
	return c, nil
}

// Run arranca el servicio y bloquea hasta que ctx se cancele.
func Run(ctx context.Context, getenv func(string) string) error {
	cfg, err := LoadConfig(getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	svc := telemetry.New(telemetry.InfoFromEnv(serviceName), slog.Default())
	log := svc.Logger
	ledger := NewLedger()
	svc.AddState("billing", func(context.Context) any { return ledger.Snapshot(false) })

	nc, err := natsx.Connect(cfg.NATS)
	if err != nil {
		return err
	}
	defer nc.Close()
	svc.AddReadyCheck("nats", func(context.Context) error { return nc.Healthy() })

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: Handler(ledger), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 3)
	go func() { errc <- svc.Serve(ctx, cfg.OpsAddr) }()
	go func() { errc <- httpSrv.ListenAndServe() }()

	biller := &Biller{Ledger: ledger, Svc: svc, Metrics: telemetry.NewDomainMetrics(svc.Reg), Log: log}
	go func() {
		// el durable lo crea mesh:up: se espera a que exista en vez de caer en bucle de reinicios
		var pc *natsx.PullConsumer
		for pc == nil {
			var err error
			pc, err = natsx.NewPullConsumer(ctx, nc, natsx.PullConfig{Stream: cfg.Stream, Durable: cfg.Durable, Logger: log, Registerer: svc.Reg})
			if err != nil {
				log.Warn("durable consumer not available yet", "stage", "start", "result", "error", "error", err)
				if !boot.Sleep(ctx, 2*time.Second) {
					return
				}
			}
		}
		log.Info("billing-sim consuming", "stage", "start", "result", "ok", "stream", cfg.Stream, "durable", cfg.Durable)
		errc <- pc.Run(ctx, biller.Handle)
	}()
	log.Info("billing-sim started", "stage", "start", "result", "ok", "addr", cfg.HTTPAddr)

	select {
	case err := <-errc:
		if err != nil {
			return err
		}
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
