// energy-transfer: consume EnergyMatched y ordena la transferencia al dispositivo.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/transfer"
)

func main() {
	logger := telemetry.SetupLogger("energy-transfer")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := transfer.Run(ctx, os.Getenv); err != nil {
		logger.Error("energy-transfer failed", "stage", "start", "result", "error", "error", err)
		stop()
		os.Exit(1)
	}
	stop()
}
