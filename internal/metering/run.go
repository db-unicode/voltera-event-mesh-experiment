package metering

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx/outboxpub"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/adapters"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// drainTimeout es lo que el apagado espera para publicar lo que quedó en la outbox: con
// KEDA en 0 réplicas nadie más lo publicaría hasta el siguiente arranque.
const drainTimeout = 15 * time.Second

// Run arma y ejecuta el gateway hasta que ctx se cancele.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	fp, err := failpoint.FromEnv()
	if err != nil {
		return fmt.Errorf("failpoints: %w", err)
	}
	svc := telemetry.New(telemetry.InfoFromEnv(ServiceName), logger)
	svc.Failpoints = fp
	go func() {
		if err := svc.Serve(ctx, cfg.OpsAddr); err != nil {
			logger.Error("ops server stopped", "stage", "start", "result", "error", "error", err)
		}
	}()

	pool, err := connectDB(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool, outbox.Migrations(), adapters.Migrations()); err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	store := adapters.NewPositionStore(pool)

	translator, err := acl.New(cfg.Zones)
	if err != nil {
		return err
	}

	pubConn := cfg.AMQP
	pubConn.ConnectionName = ServiceName + "-relay"
	rawPub := amqpx.NewPublisher(pubConn, amqpx.PublisherOptions{Logger: logger})
	defer func() { _ = rawPub.Close() }()
	relay := outbox.NewRelay(pool, outboxpub.New(rawPub), outbox.Config{Telemetry: svc, Failpoints: fp})

	gw := &Gateway{
		Region: cfg.Region, ACL: translator, Store: store, Svc: svc,
		Domain:   telemetry.NewDomainMetrics(svc.Reg),
		Rejected: NewRejectedCounter(svc.Reg),
		Delay:    cfg.ProcessingDelay, Wake: relay.Wake, Failpoints: fp,
	}
	consumer, err := amqpx.NewConsumer(amqpx.ConsumerConfig{
		Conn: cfg.AMQP, Queue: QueueDeclarations, Prefetch: cfg.Prefetch,
		EventType: EventTypeDeclaration, Metrics: svc.Metrics, Logger: logger,
	})
	if err != nil {
		return err
	}

	svc.AddReadyCheck("postgres", pool.Ping)
	svc.AddReadyCheck("consumer", consumer.Ready)
	svc.AddState("outbox", relay.StateProvider())
	svc.AddState("positions", func(ctx context.Context) any {
		n, err := store.Count(ctx)
		if err != nil {
			return map[string]string{"error": err.Error()}
		}
		return map[string]int64{"count": n}
	})
	svc.AddState("config", func(context.Context) any {
		return map[string]any{"zones": cfg.Zones, "prefetch": cfg.Prefetch, "processingDelayMs": cfg.ProcessingDelay.Milliseconds(), "vendors": translator.Vendors()}
	})

	logger.Info("declaration-gateway starting", "stage", "start", "region", cfg.Region, "vhost", cfg.AMQP.Vhost,
		"zones", cfg.Zones, "prefetch", cfg.Prefetch, "processingDelayMs", cfg.ProcessingDelay.Milliseconds())

	relayCtx, stopRelay := context.WithCancel(context.WithoutCancel(ctx))
	relayDone := make(chan struct{})
	go func() { defer close(relayDone); _ = relay.Run(relayCtx) }()
	consumeErr := consumer.Run(ctx, gw.Handle) // vuelve cuando ctx se cancela y los mensajes en curso terminaron

	svc.SetNotReady(true)
	stopRelay()
	<-relayDone
	drain(logger, relay)
	return consumeErr
}

// drain publica lo pendiente de la outbox antes de salir (acotado por drainTimeout).
func drain(logger *slog.Logger, relay *outbox.Relay) {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	total := 0
	for ctx.Err() == nil {
		n, err := relay.RunOnce(ctx)
		total += n
		if err != nil {
			logger.Warn("outbox drain stopped", "stage", "publish", "result", "error", "published", total, "error", err)
			return
		}
		if n == 0 {
			break
		}
	}
	logger.Info("outbox drained", "stage", "publish", "result", "ok", "published", total)
}

// connectDB abre el pool y espera a que la base responda (el pod puede arrancar antes).
func connectDB(ctx context.Context, dsn string, logger *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		perr := pool.Ping(ctx)
		if perr == nil {
			return pool, nil
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			pool.Close()
			return nil, fmt.Errorf("postgres not reachable: %w", perr)
		}
		logger.Warn("waiting for postgres", "stage", "start", "error", perr)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
	}
}
