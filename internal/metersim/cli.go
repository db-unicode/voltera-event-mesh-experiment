package metersim

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// Main ejecuta meter-sim y devuelve el código de salida: 0 ok, 1 fallo de publicación, 2 uso o config inválidos.
func Main(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	logger := telemetry.NewLogger(ServiceName, stderr, slog.LevelInfo)
	opts, err := ParseOptions(args, getenv, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		logger.Error("invalid configuration", "stage", "start", "result", "error", "error", err)
		return 2
	}

	info := telemetry.InfoFromEnv(ServiceName)
	info.Region = opts.Region
	svc := telemetry.New(info, logger)
	opsCtx, stopOps := context.WithCancel(ctx)
	defer stopOps()
	go func() {
		if err := svc.Serve(opsCtx, opts.OpsAddr); err != nil {
			logger.Error("ops server stopped", "stage", "start", "result", "error", "error", err)
		}
	}()

	pub := amqpx.NewPublisher(opts.AMQP, amqpx.PublisherOptions{Logger: logger})
	defer func() { _ = pub.Close() }()
	if err := waitBroker(ctx, pub, logger); err != nil {
		logger.Error("cannot reach the broker", "stage", "start", "result", "error", "error", err)
		return 1
	}
	svc.AddReadyCheck("rabbitmq", pub.Ping)

	logger.Info("meter-sim starting", "stage", "start", "profile", opts.Profile, "region", opts.Region, "vhost", opts.AMQP.Vhost,
		"rate", opts.Rate, "duration", opts.Duration.String(), "meters", opts.Meters, "zones", opts.Zones, "seed", opts.Seed,
		"vendor", opts.Vendor, "versionBase", opts.VersionBase)
	r := &Runner{Opts: opts, Sender: pub, Svc: svc, Log: logger}
	sum, err := r.Run(ctx)
	if err != nil {
		logger.Error("run failed", "stage", "publish", "result", "error", "error", err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(sum); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	logger.Info("meter-sim finished", "stage", "publish", "result", "ok", "published", sum.Published, "errors", sum.Errors,
		"late", sum.Late, "dropped", sum.Dropped, "byVendor", sum.VendorCounts())
	if sum.Errors > 0 || ctx.Err() != nil {
		return 1
	}
	return 0
}

// waitBroker reintenta la conexión inicial unos segundos (el sidecar o el broker pueden tardar).
func waitBroker(ctx context.Context, pub *amqpx.Publisher, logger *slog.Logger) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := pub.Ping(ctx)
		if err == nil || ctx.Err() != nil || time.Now().After(deadline) {
			return err
		}
		logger.Warn("waiting for the broker", "stage", "start", "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
	}
}
