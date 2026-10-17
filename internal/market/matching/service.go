package matching

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/boot"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/inbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

const serviceName = "matching-engine"

// Config se lee de variables de entorno y se valida al arrancar.
type Config struct {
	Region        string
	DatabaseURL   string
	RedisAddr     string
	RedisPassword string
	KafkaBrokers  []string
	Window        time.Duration
	RulesDir      string
	RulesPoll     time.Duration
	OpsAddr       string
	OutboxMode    OutboxMode
}

// LoadConfig lee la configuración. Vars: REGION, DATABASE_URL, REDIS_ADDR,
// REDIS_PASSWORD, KAFKA_BROKERS, MATCH_WINDOW_S (2), RULES_DIR (/etc/matching-rules),
// RULES_POLL_S (2), OPS_ADDR (:9090), OUTBOX_MODE (outbox|direct, por defecto outbox;
// direct es solo el control de E6: escritura dual sin outbox).
func LoadConfig(getenv func(string) string) (Config, error) {
	e := boot.NewEnv(getenv)
	c := Config{
		Region:        e.Region(),
		DatabaseURL:   e.Required("DATABASE_URL"),
		RedisAddr:     e.Required("REDIS_ADDR"),
		RedisPassword: e.String("REDIS_PASSWORD", ""),
		KafkaBrokers:  e.HostPorts("KAFKA_BROKERS", "kafka.voltera-infra.svc:9092"),
		Window:        e.Seconds("MATCH_WINDOW_S", 2, 1),
		RulesDir:      e.String("RULES_DIR", "/etc/matching-rules"),
		RulesPoll:     e.Seconds("RULES_POLL_S", 2, 1),
		OpsAddr:       e.Addr("OPS_ADDR", ":9090"),
	}
	err := e.Err()
	mode, merr := ParseOutboxMode(e.String("OUTBOX_MODE", "outbox"))
	c.OutboxMode = mode
	return c, errors.Join(err, merr)
}

// Run arranca el servicio y bloquea hasta que ctx se cancele.
//
// Réplicas: 1. El motor es el único escritor de matches; con N réplicas dos ventanas
// podrían emparejar las mismas posiciones (matched_positions lo descartaría, pero
// se duplicaría el trabajo). Particionar por zona (cada réplica toma un subconjunto de
// zonas por hash o por lease) permitiría N réplicas sin coordinación entre ellas.
func Run(ctx context.Context, getenv func(string) string) error {
	cfg, err := LoadConfig(getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	fp, err := failpoint.FromEnv()
	if err != nil {
		return fmt.Errorf("failpoints: %w", err)
	}
	svc := telemetry.New(telemetry.InfoFromEnv(serviceName), slog.Default())
	svc.Failpoints = fp
	log := svc.Logger
	go func() {
		if err := svc.Serve(ctx, cfg.OpsAddr); err != nil {
			log.Error("ops server failed", "stage", "start", "result", "error", "error", err)
		}
	}()

	rules, err := NewRulesLoader(cfg.RulesDir, log, svc.Reg)
	if err != nil {
		return err
	}
	go rules.Run(ctx, cfg.RulesPoll)

	pool, err := boot.ConnectPG(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool, outbox.Migrations(), inbox.Migrations(), Migrations()); err != nil {
		return err
	}
	svc.AddReadyCheck("postgres", pool.Ping)

	rdb := boot.NewRedis(cfg.RedisAddr, cfg.RedisPassword)
	defer func() { _ = rdb.Close() }()
	svc.AddReadyCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })

	pub, err := kafkax.NewPublisher(kafkax.PublisherConfig{Brokers: cfg.KafkaBrokers, ClientID: serviceName})
	if err != nil {
		return err
	}
	defer pub.Close()
	svc.AddReadyCheck("kafka", pub.Ping)

	relay := outbox.NewRelay(pool, pub, outbox.Config{Telemetry: svc, Failpoints: fp})
	svc.AddState("outbox", relay.StateProvider())
	store := NewStore(pool, cfg.Region)
	store.Mode = cfg.OutboxMode
	eng := &Engine{
		Region: cfg.Region, Window: cfg.Window, Source: ViewSource{Store: view.New(rdb, svc.Reg)},
		Store: store, Rules: rules, Metrics: telemetry.NewDomainMetrics(svc.Reg), Log: log, OnCommit: relay.Wake,
	}
	if cfg.OutboxMode == ModeDirect {
		log.Warn("OUTBOX_MODE=direct: control de E6, los matches se publican sin outbox", "stage", "start", "result", "ok")
		eng.Direct, eng.Failpoints, eng.Published = pub, fp, svc.Published
	}
	svc.AddState("rules", func(context.Context) any { return rules.State() })
	svc.AddState("lastWindow", func(context.Context) any { return eng.LastWindow() })

	log.Info("matching-engine started", "stage", "start", "result", "ok", "region", cfg.Region,
		"window", cfg.Window.String(), "topic", store.Topic(), "outboxMode", string(cfg.OutboxMode), "rules", rules.Current().Version)

	errc := make(chan error, 2)
	go func() { errc <- relay.Run(ctx) }()
	go func() { errc <- eng.Run(ctx) }()
	select {
	case err := <-errc:
		if err != nil {
			return err
		}
	case <-ctx.Done():
	}
	<-ctx.Done()
	log.Info("matching-engine stopped", "stage", "stop", "result", "ok")
	return nil
}
