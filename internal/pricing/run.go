package pricing

import (
	"context"
	"log/slog"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// Run arma y ejecuta el feed hasta que ctx se cancele.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	svc := telemetry.New(telemetry.InfoFromEnv(ServiceName), logger)
	go func() {
		if err := svc.Serve(ctx, cfg.OpsAddr); err != nil {
			logger.Error("ops server stopped", "stage", "start", "result", "error", "error", err)
		}
	}()

	pub := amqpx.NewPublisher(cfg.AMQP, amqpx.PublisherOptions{Logger: logger})
	defer func() { _ = pub.Close() }()
	svc.AddReadyCheck("rabbitmq", pub.Ping)
	feed := NewFeed(cfg.Region, NewWalk(cfg.Base, cfg.Jitter, cfg.Seed), pub, svc, cfg.MinInterval, cfg.MaxInterval, cfg.Seed)
	svc.AddState("config", func(context.Context) any {
		return map[string]any{"minIntervalSec": cfg.MinInterval.Seconds(), "maxIntervalSec": cfg.MaxInterval.Seconds(),
			"base": cfg.Base, "jitter": cfg.Jitter, "seed": cfg.Seed}
	})
	logger.Info("price-feed-simulator starting", "stage", "start", "region", cfg.Region, "vhost", cfg.AMQP.Vhost,
		"minIntervalSec", cfg.MinInterval.Seconds(), "maxIntervalSec", cfg.MaxInterval.Seconds(), "base", cfg.Base, "jitter", cfg.Jitter)
	return feed.Run(ctx)
}
