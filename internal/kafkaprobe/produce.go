package kafkaprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceOptions configura el subcomando produce.
type ProduceOptions struct {
	Topic     string
	Count     int
	KeyPrefix string
	// Rate limita los eventos por segundo; 0 = sin límite.
	Rate int
}

func (o ProduceOptions) validate() error {
	switch {
	case o.Topic == "":
		return errors.New("--topic is required")
	case o.Count < 1:
		return errors.New("--count must be >= 1")
	case o.KeyPrefix == "":
		return errors.New("--key-prefix is required")
	case o.Rate < 0:
		return errors.New("--rate must be >= 0")
	}
	return nil
}

// Producer es la parte de *kgo.Client que usa Produce.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// recordKey devuelve la clave del evento n (base 0): <prefix>-<n>.
func recordKey(prefix string, n int) string { return fmt.Sprintf("%s-%d", prefix, n) }

// buildRecords arma los eventos [from, to) del lote.
func buildRecords(topic, prefix string, from, to int) []*kgo.Record {
	rs := make([]*kgo.Record, 0, to-from)
	for n := from; n < to; n++ {
		key := recordKey(prefix, n)
		rs = append(rs, &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf(`{"eventId":%q,"seq":%d}`, key, n)),
		})
	}
	return rs
}

// batchSize y pausa entre lotes: con Rate se envían rate/10 eventos cada 100 ms.
func batchPlan(count, rate int) (size int, pause time.Duration) {
	if rate == 0 {
		return 1000, 0
	}
	size = max(rate/10, 1)
	return size, time.Second * time.Duration(size) / time.Duration(rate)
}

// Produce publica Count eventos con ProduceSync (acks=all e idempotencia vienen del cliente).
func Produce(ctx context.Context, p Producer, opts ProduceOptions, out io.Writer) error {
	if err := opts.validate(); err != nil {
		return err
	}
	size, pause := batchPlan(opts.Count, opts.Rate)
	start := time.Now()
	sent := 0
	for sent < opts.Count {
		to := min(sent+size, opts.Count)
		rs := buildRecords(opts.Topic, opts.KeyPrefix, sent, to)
		if err := p.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			return fmt.Errorf("produce after %d events: %w", sent, err)
		}
		sent = to
		if pause > 0 && sent < opts.Count {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pause):
			}
		}
	}
	_, err := fmt.Fprintf(out, "produced=%d topic=%s elapsed=%s\n", sent, opts.Topic, time.Since(start).Round(time.Millisecond))
	return err
}
