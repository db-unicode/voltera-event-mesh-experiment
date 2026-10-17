package kafkaprobe

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const usage = "usage: kafka-probe <produce|consume|work|seek> [flags]"

// Run ejecuta el subcomando args[0]. Los resultados van a out; los logs, a slog.
func Run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	cfg, err := LoadConfig(getenv)
	if err != nil {
		return err
	}
	name, rest := args[0], args[1:]
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	switch name {
	case "produce":
		var o ProduceOptions
		fs.StringVar(&o.Topic, "topic", "", "topic")
		fs.IntVar(&o.Count, "count", 0, "events to produce")
		fs.StringVar(&o.KeyPrefix, "key-prefix", "", "event key prefix")
		fs.IntVar(&o.Rate, "rate", 0, "events per second (0 = unlimited)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if err := o.validate(); err != nil {
			return err
		}
		cl, err := kgo.NewClient(append(baseOpts(cfg),
			kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all; la idempotencia es el valor por defecto de franz-go
			kgo.DefaultProduceTopic(o.Topic),
		)...)
		if err != nil {
			return fmt.Errorf("new client: %w", err)
		}
		defer cl.Close()
		return Produce(ctx, cl, o, out)

	case "consume":
		o := ConsumeOptions{Timeout: 60 * time.Second}
		fs.StringVar(&o.Topic, "topic", "", "topic")
		fs.StringVar(&o.Group, "group", "", "consumer group")
		fs.IntVar(&o.Expect, "expect", 0, "events to read before exiting")
		fs.DurationVar(&o.Timeout, "timeout", o.Timeout, "give up after this long")
		fs.StringVar(&o.KeyPrefix, "key-prefix", "", "count only keys with this prefix")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if err := o.validate(); err != nil {
			return err
		}
		cl, err := newConsumer(cfg, o.Topic, o.Group)
		if err != nil {
			return err
		}
		defer cl.Close()
		return Consume(ctx, cl, o, out)

	case "work":
		o := WorkOptions{Delay: 50 * time.Millisecond}
		fs.StringVar(&o.Topic, "topic", "", "topic")
		fs.StringVar(&o.Group, "group", "", "consumer group")
		fs.DurationVar(&o.Delay, "delay", o.Delay, "processing time per event")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if err := o.validate(); err != nil {
			return err
		}
		return runWork(ctx, cfg, o)

	case "seek":
		o := SeekOptions{To: "end"}
		fs.StringVar(&o.Topic, "topic", "", "topic")
		fs.StringVar(&o.Group, "group", "", "consumer group")
		fs.StringVar(&o.To, "to", o.To, "end|start")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if err := o.validate(); err != nil {
			return err
		}
		cl, err := kgo.NewClient(baseOpts(cfg)...)
		if err != nil {
			return fmt.Errorf("new client: %w", err)
		}
		defer cl.Close()
		return Seek(ctx, kadm.NewClient(cl), o, out)
	}
	return fmt.Errorf("unknown subcommand %q; %s", name, usage)
}

func baseOpts(cfg Config) []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("kafka-probe")}
}

func newConsumer(cfg Config, topic, group string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(append(baseOpts(cfg),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)...)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}
	return cl, nil
}

func runWork(ctx context.Context, cfg Config, o WorkOptions) error {
	consumed := newConsumedCounter(o)
	reg, err := newRegistry(consumed)
	if err != nil {
		return err
	}
	cl, err := newConsumer(cfg, o.Topic, o.Group)
	if err != nil {
		return err
	}
	defer cl.Close()

	wait := serve(ctx, cfg.MetricsAddr, newMetricsHandler(reg))
	slog.Info("worker started", "topic", o.Topic, "group", o.Group, "delay", o.Delay.String(), "metricsAddr", cfg.MetricsAddr)
	werr := Work(ctx, cl, o, consumed)
	// Work solo vuelve con ctx cancelado (el servidor ya se está cerrando) o con error.
	if werr != nil {
		return werr
	}
	return wait()
}
