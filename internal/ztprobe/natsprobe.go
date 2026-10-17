package ztprobe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// violationWait es cuánto se espera el aviso asíncrono de "Permissions Violation" tras un publish o subscribe.
const violationWait = 800 * time.Millisecond

type violations struct {
	mu   sync.Mutex
	msgs []string
}

func (v *violations) add(s string) {
	v.mu.Lock()
	v.msgs = append(v.msgs, s)
	v.mu.Unlock()
}

func (v *violations) first() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.msgs) == 0 {
		return ""
	}
	return v.msgs[0]
}

// nats: connect, publish <subject>, subscribe <subject> y consumer-info <STREAM/durable>. Un publish permitido
// llega al stream si el subject lo captura (mensaje de prueba con Nats-Msg-Id; el billing lo descarta como ilegible).
func (r *Runner) nats(ctx context.Context, c Cell) error {
	if r.Env.NATSURL == "" {
		return errors.New("NATS_URL vacío")
	}
	var v violations
	opts := []nats.Option{
		nats.Name("zt-probe-" + c.ID),
		nats.Timeout(min(4*time.Second, remaining(ctx))),
		nats.NoReconnect(),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { v.add(err.Error()) }),
	}
	if r.Env.NATSUser != "" {
		opts = append(opts, nats.UserInfo(r.Env.NATSUser, r.Env.NATSPassword))
	}
	nc, err := nats.Connect(r.Env.NATSURL, opts...)
	if err != nil {
		return fmt.Errorf("conectar: %w", err)
	}
	defer nc.Close()

	switch c.Op {
	case OpConnect:
		return nc.FlushWithContext(ctx)
	case OpPublish:
		msg := nats.NewMsg(c.Target)
		msg.Header.Set("Nats-Msg-Id", "zt-"+r.RunID+"-"+c.ID)
		msg.Data = []byte(`{"zt-probe":true}`)
		if err := nc.PublishMsg(msg); err != nil {
			return err
		}
		if err := nc.FlushWithContext(ctx); err != nil {
			return err
		}
		return waitViolation(ctx, &v)
	case OpSubscribe:
		sub, err := nc.SubscribeSync(c.Target)
		if err != nil {
			return err
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err := nc.FlushWithContext(ctx); err != nil {
			return err
		}
		return waitViolation(ctx, &v)
	case OpConsumerInfo:
		stream, durable, ok := strings.Cut(c.Target, "/")
		if !ok || stream == "" || durable == "" {
			return fmt.Errorf("target %q: se espera STREAM/durable", c.Target)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			return err
		}
		if _, err := js.Consumer(ctx, stream, durable); err != nil {
			if m := v.first(); m != "" {
				return errors.New(m)
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("operación %q no soportada", c.Op)
}

func waitViolation(ctx context.Context, v *violations) error {
	select {
	case <-time.After(violationWait):
	case <-ctx.Done():
		return ctx.Err()
	}
	if m := v.first(); m != "" {
		return errors.New(m)
	}
	return nil
}
