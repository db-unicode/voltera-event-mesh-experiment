package amqpprobe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// confirmWindow acota los mensajes sin confirmar; al llenarse se espera su confirmación.
const confirmWindow = 500

// PublishOptions define qué y cuánto publicar.
type PublishOptions struct {
	Exchange   string
	RoutingKey string
	Count      int
}

// PublishResult resume una publicación. Published cuenta los envíos; Confirmed, los ack del broker;
// Returned, los mensajes que el broker devolvió por no tener cola destino (mandatory).
type PublishResult struct {
	Published int           `json:"published"`
	Confirmed int           `json:"confirmed"`
	Nacked    int           `json:"nacked"`
	Returned  int           `json:"returned"`
	Elapsed   time.Duration `json:"-"`
	ElapsedMS int64         `json:"elapsedMs"`
}

// OK es verdadero si todos los envíos fueron confirmados y ninguno fue devuelto.
func (r PublishResult) OK(want int) bool {
	return r.Published == want && r.Confirmed == want && r.Nacked == 0 && r.Returned == 0
}

// Publish envía opts.Count mensajes persistentes con publisher confirms y mandatory.
// Si el broker cierra el canal (p. ej. 403 ACCESS_REFUSED por permisos) devuelve ese error.
func Publish(ctx context.Context, conn *amqp.Connection, opts PublishOptions) (PublishResult, error) {
	var res PublishResult
	if opts.Count < 1 {
		return res, errors.New("--count debe ser >= 1")
	}
	ch, err := conn.Channel()
	if err != nil {
		return res, fmt.Errorf("abrir canal: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Confirm(false); err != nil {
		return res, fmt.Errorf("activar confirms: %w", err)
	}

	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	var returned atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	returns := ch.NotifyReturn(make(chan amqp.Return, 16))
	go func() {
		defer wg.Done()
		for range returns {
			returned.Add(1)
		}
	}()

	start := time.Now()
	runID := newRunID()
	pending := make([]*amqp.DeferredConfirmation, 0, confirmWindow)
	settle := func() error {
		for _, dc := range pending {
			ok, err := dc.WaitContext(ctx)
			if err != nil {
				return err
			}
			if ok {
				res.Confirmed++
			} else {
				res.Nacked++
			}
		}
		pending = pending[:0]
		return nil
	}

	for i := 1; i <= opts.Count; i++ {
		body, err := buildBody(runID, i, time.Now())
		if err != nil {
			return res, err
		}
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, opts.Exchange, opts.RoutingKey, true, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    fmt.Sprintf("%s-%d", runID, i),
			Timestamp:    time.Now(),
			Body:         body,
		})
		if err != nil {
			return res, channelError(err, closed)
		}
		res.Published++
		pending = append(pending, dc)
		if len(pending) >= confirmWindow {
			if err := settle(); err != nil {
				return res, err
			}
		}
	}
	if err := settle(); err != nil {
		return res, err
	}
	if res.Nacked > 0 {
		return res, channelError(fmt.Errorf("%d mensajes sin ack del broker", res.Nacked), closed)
	}

	_ = ch.Close()
	wg.Wait()
	res.Returned = int(returned.Load())
	res.Elapsed = time.Since(start)
	res.ElapsedMS = res.Elapsed.Milliseconds()
	return res, nil
}

// channelError agrega el motivo del cierre del canal (si lo hay) al error original. El broker
// cierra el canal con 403 al publicar en un exchange sin permiso `write`.
func channelError(err error, closed <-chan *amqp.Error) error {
	select {
	case ce := <-closed:
		if ce != nil {
			return fmt.Errorf("%w: el broker cerró el canal: %w", err, ce)
		}
	case <-time.After(2 * time.Second):
	}
	return err
}
