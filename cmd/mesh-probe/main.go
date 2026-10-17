// mesh-probe publica y consume por NATS JetStream para probar el event mesh.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/meshprobe"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(meshprobe.Run(ctx, os.Args[1:], os.Getenv, os.Stdout))
}
