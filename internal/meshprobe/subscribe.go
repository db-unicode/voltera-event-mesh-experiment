package meshprobe

import (
	"context"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SubscribeOptions configura subscribe. Con Core=true usa una suscripción NATS simple
// (sin JetStream); si no, un durable pull consumer.
type SubscribeOptions struct {
	Stream  string
	Durable string
	Subject string // solo en modo Core
	Core    bool
	Expect  int           // 0 = esperar Timeout completo (para comprobar que no llega nada)
	Timeout time.Duration // tope total
	Linger  time.Duration // espera extra tras alcanzar Expect para detectar sobrantes
}

// SubscribeResult resume una ejecución de subscribe.
type SubscribeResult struct {
	Received      int            `json:"received"`
	Distinct      int            `json:"distinct"`
	Duplicates    int            `json:"duplicates"`
	ByPrefix      map[string]int `json:"by_prefix"`
	Violations    int            `json:"violations"`
	ViolationMsgs []string       `json:"violation_msgs,omitempty"`
	FirstError    string         `json:"first_error,omitempty"`
	ElapsedMS     int64          `json:"elapsed_ms"`
}

type tally struct {
	res  SubscribeResult
	seen map[string]struct{}
}

func newTally() *tally {
	return &tally{res: SubscribeResult{ByPrefix: map[string]int{}}, seen: map[string]struct{}{}}
}

// add registra un mensaje; identifica por Nats-Msg-Id (o por subject si no lo trae).
func (t *tally) add(subject string, h nats.Header) {
	id := h.Get(jetstream.MsgIDHeader)
	if id == "" {
		id = subject
	}
	t.res.Received++
	if _, dup := t.seen[id]; dup {
		t.res.Duplicates++
	} else {
		t.seen[id] = struct{}{}
		t.res.Distinct++
	}
	t.res.ByPrefix[prefix(subject, 3)]++
}

// prefix devuelve los primeros n tokens del subject (energy.matched.r1 para energy.matched.r1.x).
func prefix(subject string, n int) string {
	parts := strings.Split(subject, ".")
	if len(parts) > n {
		parts = parts[:n]
	}
	return strings.Join(parts, ".")
}

// Subscribe consume mensajes y cuenta recibidos y distintos por Nats-Msg-Id.
func (c *Client) Subscribe(ctx context.Context, o SubscribeOptions) SubscribeResult {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	t := newTally()
	if o.Core {
		c.subscribeCore(ctx, o, t)
	} else {
		c.subscribeDurable(ctx, o, t)
	}
	t.res.ViolationMsgs = c.Violations()
	t.res.Violations = len(t.res.ViolationMsgs)
	t.res.ElapsedMS = time.Since(start).Milliseconds()
	return t.res
}

func (c *Client) subscribeDurable(ctx context.Context, o SubscribeOptions, t *tally) {
	cons, err := c.js.Consumer(ctx, o.Stream, o.Durable)
	if err != nil {
		t.res.FirstError = err.Error()
		return
	}
	for ctx.Err() == nil {
		wait := 2 * time.Second
		reached := o.Expect > 0 && t.res.Received >= o.Expect
		if reached {
			wait = o.Linger
		}
		batch, err := cons.Fetch(100, jetstream.FetchMaxWait(wait))
		if err != nil {
			t.res.FirstError = err.Error()
			return
		}
		got := 0
		for msg := range batch.Messages() {
			t.add(msg.Subject(), msg.Headers())
			if err := msg.DoubleAck(ctx); err != nil && t.res.FirstError == "" {
				t.res.FirstError = err.Error()
			}
			got++
		}
		if err := batch.Error(); err != nil && ctx.Err() == nil && t.res.FirstError == "" {
			t.res.FirstError = err.Error()
		}
		if reached && got == 0 {
			return
		}
	}
}

func (c *Client) subscribeCore(ctx context.Context, o SubscribeOptions, t *tally) {
	ch := make(chan *nats.Msg, 1024)
	sub, err := c.nc.ChanSubscribe(o.Subject, ch)
	if err != nil {
		t.res.FirstError = err.Error()
		return
	}
	defer func() { _ = sub.Unsubscribe() }()
	// El servidor rechaza la suscripción de forma asíncrona: forzar el viaje de ida y vuelta.
	if err := c.nc.Flush(); err != nil {
		t.res.FirstError = err.Error()
	}
	c.waitViolation(ctx, 0, 500*time.Millisecond)
	if c.violationCount() > 0 {
		return
	}
	for {
		select {
		case m := <-ch:
			t.add(m.Subject, m.Header)
			if o.Expect > 0 && t.res.Received >= o.Expect {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
