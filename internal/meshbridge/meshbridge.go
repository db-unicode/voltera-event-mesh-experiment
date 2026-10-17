// Package meshbridge es mesh-bridge: reenvía los EnergyMatched de Kafka (<REGION>.energy-matched)
// al nodo NATS regional, desde donde el leafnode los lleva al stream global
// BILLING_MATCHED (ADR 0005).
//
// El offset de Kafka se confirma solo después del PubAck del stream. Sin PubAck (enlace
// caído, stream no disponible, permiso denegado) el mensaje no se confirma: el consumidor
// espera con backoff y reintenta el MISMO mensaje, de modo que el orden de la partición
// se conserva y no se pierde ningún evento. Un reenvío tras un crash lleva el mismo
// Nats-Msg-Id (matchId) y el stream lo descarta dentro de su duplicate_window (2 min).
package meshbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/boot"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/natsx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

const serviceName = "mesh-bridge"

// ConsumerGroup es el grupo de Kafka del bridge.
const ConsumerGroup = "mesh-bridge"

// Subject es el subject NATS de un match: energy.matched.<region>.<matchId>.
func Subject(region, matchID string) string { return "energy.matched." + region + "." + matchID }

// PublishFunc publica y devuelve nil solo con el PubAck del stream.
type PublishFunc func(ctx context.Context, m natsx.Msg) error

// Bridge convierte registros de Kafka en publicaciones NATS confirmadas.
type Bridge struct {
	Region  string
	Publish PublishFunc
	Svc     *telemetry.Service
	Metrics *telemetry.DomainMetrics
	Log     *slog.Logger
}

// Handle implementa kafkax.Handler: error ⇒ sin PubAck ⇒ el consumidor reintenta.
func (b *Bridge) Handle(ctx context.Context, rec kafkax.Record) error {
	env, m, err := parse(rec)
	if err != nil {
		// mensaje venenoso: reintentar no lo arregla y bloquearía la partición
		b.Log.Error("poison message skipped", "stage", "consume", "result", telemetry.ResultError,
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "error", err)
		return nil
	}
	tp := rec.Headers[kafkax.HeaderTraceparent]
	if tp == "" {
		tp = env.Traceparent
	}
	env.Traceparent = tp
	telemetry.LogStage(b.Log, telemetry.StageConsume, env, telemetry.ResultOK,
		"matchId", m.MatchID, "partition", rec.Partition, "offset", rec.Offset)

	subject := Subject(b.Region, m.MatchID)
	start := time.Now()
	err = b.Publish(ctx, natsx.Msg{
		Subject: subject, MsgID: m.MatchID, Header: map[string]string{kafkax.HeaderTraceparent: tp}, Data: rec.Value,
	})
	if err != nil {
		reason := natsx.ReasonOther
		var rej *natsx.RejectedError
		if errors.As(err, &rej) {
			reason = rej.Reason
		}
		b.Metrics.MeshPublishRejected.WithLabelValues(b.Region, reason).Inc()
		telemetry.LogStage(b.Log, telemetry.StagePublish, env, telemetry.ResultError,
			"matchId", m.MatchID, "subject", subject, "reason", reason, "error", err)
		return err
	}
	b.Metrics.MeshPublished.WithLabelValues(b.Region).Inc()
	b.Svc.Published(env, "nats")
	b.Svc.Consumed(env, rec.Topic, telemetry.ResultOK)
	telemetry.LogStage(b.Log, telemetry.StagePublish, env, telemetry.ResultOK,
		"matchId", m.MatchID, "subject", subject, "ackMs", time.Since(start).Milliseconds())
	return nil
}

func parse(rec kafkax.Record) (events.Envelope, events.EnergyMatched, error) {
	env, err := events.Unmarshal(rec.Value)
	if err != nil {
		return env, events.EnergyMatched{}, err
	}
	if env.Type != events.TypeEnergyMatched {
		return env, events.EnergyMatched{}, fmt.Errorf("unexpected event type %q", env.Type)
	}
	m, err := events.DecodePayload[events.EnergyMatched](env)
	if err != nil {
		return env, m, err
	}
	// el matchId va en un token del subject: nada de separadores ni comodines
	if m.MatchID == "" || strings.ContainsAny(m.MatchID, ".*> \t\r\n") {
		return env, m, fmt.Errorf("invalid matchId %q", m.MatchID)
	}
	return env, m, nil
}

// Config se lee de variables de entorno y se valida al arrancar.
type Config struct {
	Region       string
	KafkaBrokers []string
	NATS         natsx.Config
	AckTimeout   time.Duration
	OpsAddr      string
}

// Topic es el topic de entrada de la región.
func (c Config) Topic() string { return c.Region + ".energy-matched" }

// LoadConfig lee la configuración. Vars: REGION, KAFKA_BROKERS, NATS_URL, NATS_USER,
// NATS_PASSWORD, ACK_TIMEOUT_S (5), OPS_ADDR (:9090).
func LoadConfig(getenv func(string) string) (Config, error) {
	e := boot.NewEnv(getenv)
	c := Config{
		Region:       e.Region(),
		KafkaBrokers: e.HostPorts("KAFKA_BROKERS", "kafka.voltera-infra.svc:9092"),
		NATS:         natsx.Config{URL: e.Required("NATS_URL"), User: e.Required("NATS_USER"), Password: e.Required("NATS_PASSWORD"), Name: serviceName},
		AckTimeout:   e.Seconds("ACK_TIMEOUT_S", 5, 1),
		OpsAddr:      e.Addr("OPS_ADDR", ":9090"),
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
	go func() {
		if err := svc.Serve(ctx, cfg.OpsAddr); err != nil {
			log.Error("ops server failed", "stage", "start", "result", "error", "error", err)
		}
	}()

	nc, err := natsx.Connect(cfg.NATS)
	if err != nil {
		return err
	}
	defer nc.Close()
	svc.AddReadyCheck("nats", func(context.Context) error { return nc.Healthy() })
	pub := natsx.NewPublisher(nc, cfg.AckTimeout)

	b := &Bridge{
		Region: cfg.Region, Svc: svc, Metrics: telemetry.NewDomainMetrics(svc.Reg), Log: log,
		Publish: func(ctx context.Context, m natsx.Msg) error { _, err := pub.Publish(ctx, m); return err },
	}
	cons, err := kafkax.NewConsumer(kafkax.ConsumerConfig{
		Brokers: cfg.KafkaBrokers, ClientID: serviceName, Group: ConsumerGroup, Topic: cfg.Topic(),
		Logger: log, Registerer: svc.Reg,
	})
	if err != nil {
		return err
	}
	svc.AddReadyCheck("kafka", cons.Ping)

	log.Info("mesh-bridge started", "stage", "start", "result", "ok", "region", cfg.Region,
		"topic", cfg.Topic(), "nats", cfg.NATS.URL, "user", cfg.NATS.User)
	err = cons.Run(ctx, b.Handle)
	cons.Close()
	log.Info("mesh-bridge stopped", "stage", "stop", "result", "ok")
	return err
}
