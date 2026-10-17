// view-probe: spike S7 (vista Redis con TTL, versión y recuperación).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	probe "github.com/db-unicode/voltera-event-mesh-experiment/internal/probes/view"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("view-probe")
	cfg, err := probe.ConfigFromEnv()
	if err != nil {
		logger.Error("config", "stage", "start", "result", "error", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failed, err := probe.Run(ctx, cfg, os.Stdout)
	if err != nil {
		logger.Error("view-probe failed", "stage", "run", "result", "error", "error", err)
		os.Exit(1)
	}
	if failed > 0 {
		fmt.Printf("RESULT FAIL %d checks\n", failed)
		os.Exit(1)
	}
	fmt.Println("RESULT PASS")
}
