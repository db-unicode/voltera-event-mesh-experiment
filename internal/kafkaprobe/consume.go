package kafkaprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const pollBatch = 500

// ConsumeOptions configura el subcomando consume.
type ConsumeOptions struct {
	Topic   string
	Group   string
	Expect  int
	Timeout time.Duration
	// KeyPrefix, si no está vacío, cuenta solo las claves con ese prefijo.
	KeyPrefix string
}

func (o ConsumeOptions) validate() error {
	switch {
	case o.Topic == "":
		return errors.New("--topic is required")
	case o.Group == "":
		return errors.New("--group is required")
	case o.Expect < 1:
		return errors.New("--expect must be >= 1")
	case o.Timeout <= 0:
		return errors.New("--timeout must be > 0")
	}
	return nil
}

// Consumer es la parte de *kgo.Client que usan Consume y Work.
type Consumer interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
}

// Tally cuenta eventos y claves distintas.
type Tally struct {
	prefix   string
	count    int
	distinct map[string]struct{}
}

// NewTally cuenta solo claves con el prefijo dado ("" = todas).
func NewTally(prefix string) *Tally {
	return &Tally{prefix: prefix, distinct: make(map[string]struct{})}
}

// Add registra el evento si su clave coincide con el prefijo.
func (t *Tally) Add(r *kgo.Record) {
	key := string(r.Key)
	if !strings.HasPrefix(key, t.prefix) {
		return
	}
	t.count++
	t.distinct[key] = struct{}{}
}

func (t *Tally) Count() int    { return t.count }
func (t *Tally) Distinct() int { return len(t.distinct) }

// pollRecords devuelve los registros de un poll; el error es solo de contexto cancelado.
func pollRecords(ctx context.Context, c Consumer, n int) ([]*kgo.Record, error) {
	fetches := c.PollRecords(ctx, n)
	if ctx.Err() != nil {
		return fetches.Records(), ctx.Err()
	}
	if fetches.IsClientClosed() {
		return nil, errors.New("kafka client closed")
	}
	return fetches.Records(), nil
}

// Consume lee hasta Expect eventos (o Timeout), con commit manual tras procesar cada lote.
// Imprime "count=N distinct=M". Falla si count < Expect.
func Consume(ctx context.Context, c Consumer, opts ConsumeOptions, out io.Writer) error {
	if err := opts.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	tally := NewTally(opts.KeyPrefix)
	var pollErr error
	for tally.Count() < opts.Expect {
		rs, err := pollRecords(ctx, c, pollBatch)
		for _, r := range rs {
			tally.Add(r)
		}
		if len(rs) > 0 {
			// el commit usa un contexto propio: el timeout del consumo no debe perder el último lote
			if cerr := commit(c, rs); cerr != nil {
				pollErr = cerr
				break
			}
		}
		c.AllowRebalance()
		if err != nil {
			pollErr = err
			break
		}
	}
	if _, err := fmt.Fprintf(out, "count=%d distinct=%d expect=%d group=%s topic=%s\n",
		tally.Count(), tally.Distinct(), opts.Expect, opts.Group, opts.Topic); err != nil {
		return err
	}
	if tally.Count() < opts.Expect {
		return fmt.Errorf("got %d of %d events before timeout %s: %w", tally.Count(), opts.Expect, opts.Timeout, pollErr)
	}
	return nil
}

func commit(c Consumer, rs []*kgo.Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.CommitRecords(ctx, rs...); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
