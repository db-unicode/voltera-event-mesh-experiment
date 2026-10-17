// metrics-probe: spike S13 (métricas Go raspadas por el Prometheus de Istio).
package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	probe "github.com/db-unicode/voltera-event-mesh-experiment/internal/probes/metrics"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("metrics-probe")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rate := 5
	if v := os.Getenv("RATE_HZ"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			logger.Error("RATE_HZ must be an integer >= 1", "stage", "start", "result", "error")
			os.Exit(1)
		}
		rate = n
	}
	addr := os.Getenv("OPS_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	svc := telemetry.New(telemetry.InfoFromEnv("metrics-probe"), logger)
	go func() { _ = probe.Generate(ctx, svc, time.Second/time.Duration(rate)) }()
	if err := svc.Serve(ctx, addr); err != nil {
		logger.Error("ops server failed", "stage", "run", "result", "error", "error", err)
		os.Exit(1)
	}
}
