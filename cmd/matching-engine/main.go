// matching-engine: empareja posiciones SURPLUS/DEFICIT de la vista Redis con reglas expr.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/market/matching"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("matching-engine")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := matching.Run(ctx, os.Getenv); err != nil {
		logger.Error("matching-engine failed", "stage", "start", "result", "error", "error", err)
		stop()
		os.Exit(1)
	}
}
