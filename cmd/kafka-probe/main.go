// kafka-probe: cliente de prueba para los spikes de Kafka (produce, consume, work, seek).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/kafkaprobe"
)

func main() {
	os.Exit(run())
}

func run() int {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := kafkaprobe.Run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		slog.Error("kafka-probe failed", "error", err)
		return 1
	}
	return 0
}
