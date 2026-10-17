// device-sim: dispositivo simulado que ejecuta comandos de transferencia firmados.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/devicesim"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("device-sim")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := devicesim.Run(ctx, os.Getenv); err != nil {
		logger.Error("device-sim failed", "stage", "start", "result", "error", "error", err)
		stop()
		os.Exit(1)
	}
	stop()
}
