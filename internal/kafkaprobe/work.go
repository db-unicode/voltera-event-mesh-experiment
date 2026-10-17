package kafkaprobe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// WorkOptions configura el subcomando work.
type WorkOptions struct {
	Topic string
	Group string
	// Delay simula el trabajo por evento (comando al dispositivo en energy-transfer).
	Delay time.Duration
}

func (o WorkOptions) validate() error {
	switch {
	case o.Topic == "":
		return errors.New("--topic is required")
	case o.Group == "":
		return errors.New("--group is required")
	case o.Delay < 0:
		return errors.New("--delay must be >= 0")
	}
	return nil
}

// newConsumedCounter crea events_consumed_total con el topic y el grupo como etiquetas fijas.
func newConsumedCounter(opts WorkOptions) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "events_consumed_total",
		Help:        "Events processed and committed by this consumer.",
		ConstLabels: prometheus.Labels{"topic": opts.Topic, "group": opts.Group},
	})
}

// Work consume de forma continua hasta que ctx se cancela. Cada evento tarda Delay;
// el commit es manual tras cada lote. Al cancelar, confirma solo lo ya procesado.
func Work(ctx context.Context, c Consumer, opts WorkOptions, consumed prometheus.Counter) error {
	if err := opts.validate(); err != nil {
		return err
	}
	for ctx.Err() == nil {
		rs, err := pollRecords(ctx, c, 50)
		if err != nil && ctx.Err() == nil {
			return err
		}
		done := 0
		for range rs {
			if err := sleep(ctx, opts.Delay); err != nil {
				break
			}
			done++
			consumed.Inc()
		}
		if done > 0 {
			if err := commit(c, rs[:done]); err != nil {
				return err
			}
		}
		c.AllowRebalance()
	}
	slog.Info("worker stopped", "topic", opts.Topic, "group", opts.Group)
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("sleep: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}
