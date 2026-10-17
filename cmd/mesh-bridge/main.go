// mesh-bridge: reenvía los EnergyMatched de Kafka al event mesh NATS con PubAck.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/meshbridge"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("mesh-bridge")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := meshbridge.Run(ctx, os.Getenv); err != nil {
		logger.Error("mesh-bridge failed", "stage", "start", "result", "error", "error", err)
		stop()
		os.Exit(1)
	}
	stop()
}
