package projection

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

// Run arma y ejecuta el proyector hasta que ctx se cancele.
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer func() { _ = rdb.Close() }()
	proj := &Projector{
		Region: cfg.Region, View: view.New(rdb, svc.Reg), Svc: svc,
		PositionTTL: cfg.PositionTTL, PriceTTL: cfg.PriceTTL,
		Failpoints: fp, Dropped: NewDroppedCounter(svc.Reg),
	}

	newConsumer := func(queue, eventType string) (*amqpx.Consumer, error) {
		conn := cfg.AMQP
		conn.ConnectionName = ServiceName + "-" + queue
		return amqpx.NewConsumer(amqpx.ConsumerConfig{
			Conn: conn, Queue: queue, Prefetch: cfg.Prefetch, EventType: eventType, Metrics: svc.Metrics, Logger: logger,
		})
	}
	positions, err := newConsumer(QueuePositions, "PositionDeclared")
	if err != nil {
		return err
	}
	prices, err := newConsumer(QueuePrices, "PriceUpdated")
	if err != nil {
		return err
	}
	svc.AddReadyCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	svc.AddReadyCheck("energy-positions", positions.Ready)
	svc.AddReadyCheck("kwh-prices", prices.Ready)
	svc.AddState("config", func(context.Context) any {
		return map[string]any{"viewTtlSec": cfg.PositionTTL.Seconds(), "priceTtlSec": cfg.PriceTTL.Seconds(), "prefetch": cfg.Prefetch, "redis": cfg.RedisAddr}
	})

	logger.Info("market-projector starting", "stage", "start", "region", cfg.Region, "vhost", cfg.AMQP.Vhost,
		"redis", cfg.RedisAddr, "viewTtlSec", cfg.PositionTTL.Seconds(), "priceTtlSec", cfg.PriceTTL.Seconds())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = positions.Run(ctx, proj.HandlePosition) }()
	go func() { defer wg.Done(); _ = prices.Run(ctx, proj.HandlePrice) }()
	wg.Wait()
	svc.SetNotReady(true)
	return nil
}
