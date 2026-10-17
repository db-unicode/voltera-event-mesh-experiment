// market-projector: consume energy-positions y kwh-prices y mantiene la vista Redis market-view.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/market/projection"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger(projection.ServiceName)
	cfg, err := projection.ConfigFromEnv(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "stage", "start", "result", "error", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := projection.Run(ctx, cfg, logger); err != nil {
		logger.Error("market-projector failed", "stage", "start", "result", "error", "error", err)
		os.Exit(1)
	}
}
