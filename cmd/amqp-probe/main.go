// amqp-probe publica y consume en RabbitMQ para los spikes S4 y S5.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/amqpprobe"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := amqpprobe.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}
