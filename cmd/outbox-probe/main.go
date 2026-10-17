// outbox-probe: spike S6 (outbox transaccional con relay y failpoints).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	probe "github.com/db-unicode/voltera-event-mesh-experiment/internal/probes/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func main() {
	logger := telemetry.SetupLogger("outbox-probe")
	if err := run(); err != nil {
		logger.Error("outbox-probe failed", "stage", "start", "result", "error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := probe.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	fp, err := failpoint.FromEnv()
	if err != nil {
		return fmt.Errorf("failpoints: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc := telemetry.New(telemetry.InfoFromEnv("outbox-probe"), nil)
	svc.Failpoints = fp
	addr := os.Getenv("OPS_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	go func() { _ = svc.Serve(ctx, addr) }()

	res, err := probe.Run(ctx, cfg, svc, fp)
	if err != nil {
		return err
	}
	res.Log(svc.Logger)
	if res.Pending != 0 || res.SinkDistinct != int64(cfg.Rows) {
		return fmt.Errorf("verification failed: %+v", res)
	}
	return nil
}
