// billing-sim: Facturación simulada; consume el durable "billing" del stream BILLING_MATCHED.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/billingsim"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("billing-sim")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := billingsim.Run(ctx, os.Getenv); err != nil {
		logger.Error("billing-sim failed", "stage", "start", "result", "error", "error", err)
		stop()
		os.Exit(1)
	}
	stop()
}
