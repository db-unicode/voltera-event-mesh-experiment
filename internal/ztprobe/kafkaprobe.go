package ztprobe

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafka: target = topic. Istio decide por identidad y puerto, no por topic: el resultado de una celda es el mismo
// para cualquier topic. publish escribe un registro de prueba (los consumidores reales lo saltan por ilegible);
// consume lee los offsets finales y hace un fetch desde el final (sin grupo, sin confirmar nada).
func (r *Runner) kafka(ctx context.Context, c Cell) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(r.Env.KafkaBrokers...),
		kgo.ClientID("zt-probe-"+c.ID),
		kgo.DialTimeout(3*time.Second),
		kgo.RequestRetries(1),
		kgo.RetryTimeout(remaining(ctx)),
		kgo.ConsumeTopics(c.Target),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	switch c.Op {
	case OpPublish:
		res := cl.ProduceSync(ctx, &kgo.Record{
			Topic: c.Target, Key: []byte("zt-probe-" + r.RunID + "-" + c.ID),
			Value:   []byte(`{"zt-probe":true}`),
			Headers: []kgo.RecordHeader{{Key: "zt-probe", Value: []byte("1")}},
		})
		return res.FirstErr()
	case OpConsume:
		offs, err := kadm.NewClient(cl).ListEndOffsets(ctx, c.Target)
		if err != nil {
			return fmt.Errorf("offsets: %w", err)
		}
		if err := offs.Error(); err != nil {
			return fmt.Errorf("offsets: %w", err)
		}
		fctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer cancel()
		for _, fe := range cl.PollFetches(fctx).Errors() {
			if !errors.Is(fe.Err, context.DeadlineExceeded) && fctx.Err() == nil {
				return fmt.Errorf("fetch: %w", fe.Err)
			}
		}
		return nil
	}
	return fmt.Errorf("operación %q no soportada", c.Op)
}
