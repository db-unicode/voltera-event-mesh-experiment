// declaration-gateway: consume meter-declarations, traduce con la ACL y guarda posiciones + outbox.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger(metering.ServiceName)
	cfg, err := metering.ConfigFromEnv(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "stage", "start", "result", "error", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := metering.Run(ctx, cfg, logger); err != nil {
		logger.Error("declaration-gateway failed", "stage", "start", "result", "error", "error", err)
		os.Exit(1)
	}
}
