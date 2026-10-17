package kafkaprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/w3c"
)

// MatchedOptions configura el subcomando produce-matched: EnergyMatched válidos con
// medidores <prefix>-s<n> (oferente) y <prefix>-b<n> (demandante), para llenar el topic
// sin pasar por matching-engine (experimento E2 parte B).
type MatchedOptions struct {
	Topic  string
	Region string
	Count  int
	Prefix string
	// Rate limita los eventos por segundo; 0 = sin límite.
	Rate int
}

func (o MatchedOptions) validate() error {
	switch {
	case o.Topic == "":
		return errors.New("--topic is required")
	case o.Region == "":
		return errors.New("--region is required")
	case o.Count < 1:
		return errors.New("--count must be >= 1")
	case o.Prefix == "":
		return errors.New("--prefix is required")
	case o.Rate < 0:
		return errors.New("--rate must be >= 0")
	}
	return nil
}

// buildMatchedRecords arma los EnergyMatched [from, to) del lote. El matchId es
// determinista (events.MatchID) y es la clave del registro, como en el relay de matching-engine.
func buildMatchedRecords(o MatchedOptions, from, to int, now time.Time) ([]*kgo.Record, error) {
	rs := make([]*kgo.Record, 0, to-from)
	window := o.Prefix + "-window"
	for n := from; n < to; n++ {
		seller := fmt.Sprintf("%s-s%d", o.Prefix, n)
		buyer := fmt.Sprintf("%s-b%d", o.Prefix, n)
		m := events.EnergyMatched{
			MatchID:       events.MatchID(window, seller+"@1", buyer+"@1"),
			SellerMeterID: seller,
			BuyerMeterID:  buyer,
			KWh:           1,
			PricePerKWh:   0.15,
			Zone:          "z-1",
			Window:        window,
		}
		env, err := events.NewEnergyMatched(o.Region, 1, now, m)
		if err != nil {
			return nil, err
		}
		env.CommittedAt, env.EmittedAt = now.UTC(), now.UTC()
		env.Traceparent = w3c.NewTraceparent()
		body, err := events.Marshal(env)
		if err != nil {
			return nil, err
		}
		rs = append(rs, &kgo.Record{
			Topic: o.Topic, Key: []byte(m.MatchID), Value: body,
			Headers: []kgo.RecordHeader{
				{Key: kafkax.HeaderEventID, Value: []byte(env.EventID)},
				{Key: kafkax.HeaderType, Value: []byte(env.Type)},
				{Key: kafkax.HeaderTraceparent, Value: []byte(env.Traceparent)},
			},
		})
	}
	return rs, nil
}

// ProduceMatched publica Count EnergyMatched con ProduceSync.
func ProduceMatched(ctx context.Context, p Producer, o MatchedOptions, out io.Writer) error {
	if err := o.validate(); err != nil {
		return err
	}
	size, pause := batchPlan(o.Count, o.Rate)
	start := time.Now()
	sent := 0
	for sent < o.Count {
		to := min(sent+size, o.Count)
		rs, err := buildMatchedRecords(o, sent, to, time.Now())
		if err != nil {
			return err
		}
		if err := p.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			return fmt.Errorf("produce-matched after %d events: %w", sent, err)
		}
		sent = to
		if pause > 0 && sent < o.Count {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pause):
			}
		}
	}
	_, err := fmt.Fprintf(out, "produced=%d topic=%s elapsed=%s\n", sent, o.Topic, time.Since(start).Round(time.Millisecond))
	return err
}
