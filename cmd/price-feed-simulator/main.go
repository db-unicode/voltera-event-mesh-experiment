// price-feed-simulator: publica PriceUpdated de la región en kwh-prices a intervalos aleatorios.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/pricing"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger(pricing.ServiceName)
	cfg, err := pricing.ConfigFromEnv(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "stage", "start", "result", "error", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := pricing.Run(ctx, cfg, logger); err != nil {
		logger.Error("price-feed-simulator failed", "stage", "start", "result", "error", "error", err)
		os.Exit(1)
	}
}
