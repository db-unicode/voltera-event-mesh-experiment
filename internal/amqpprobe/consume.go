package amqpprobe

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumeOptions define cuántos mensajes leer y con qué prefetch.
type ConsumeOptions struct {
	Queue    string
	Expect   int
	Prefetch int
}

// ConsumeResult resume un consumo. Redelivered cuenta los mensajes marcados como reentregados.
type ConsumeResult struct {
	Consumed    int   `json:"consumed"`
	Redelivered int   `json:"redelivered"`
	ElapsedMS   int64 `json:"elapsedMs"`
}

// Consume lee exactamente opts.Expect mensajes con ack manual y vuelve. Falla si el contexto vence antes.
func Consume(ctx context.Context, conn *amqp.Connection, opts ConsumeOptions) (ConsumeResult, error) {
	var res ConsumeResult
	if opts.Expect < 1 {
		return res, errors.New("--expect debe ser >= 1")
	}
	ch, err := conn.Channel()
	if err != nil {
		return res, fmt.Errorf("abrir canal: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Qos(clampPrefetch(opts.Prefetch, opts.Expect), 0, false); err != nil {
		return res, fmt.Errorf("qos: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	tag := "amqp-probe-" + newRunID()
	deliveries, err := ch.ConsumeWithContext(ctx, opts.Queue, tag, false, false, false, false, nil)
	if err != nil {
		return res, fmt.Errorf("consumir %q: %w", opts.Queue, err)
	}

	start := time.Now()
	for res.Consumed < opts.Expect {
		select {
		case <-ctx.Done():
			return res, fmt.Errorf("se esperaban %d mensajes y llegaron %d: %w", opts.Expect, res.Consumed, ctx.Err())
		case ce := <-closed:
			return res, fmt.Errorf("tras %d mensajes: %w", res.Consumed, closeErr(ce))
		case d, ok := <-deliveries:
			if !ok {
				return res, fmt.Errorf("consumidor cancelado tras %d mensajes", res.Consumed)
			}
			if err := d.Ack(false); err != nil {
				return res, fmt.Errorf("ack: %w", err)
			}
			res.Consumed++
			if d.Redelivered {
				res.Redelivered++
			}
		}
	}
	// Lo que el broker entregó de más (prefetch) vuelve a la cola al cerrar el canal.
	_ = ch.Cancel(tag, false)
	res.ElapsedMS = time.Since(start).Milliseconds()
	return res, nil
}

// clampPrefetch evita un prefetch menor que 1 (ilimitado en AMQP) o mayor que lo esperado.
func clampPrefetch(prefetch, expect int) int {
	if prefetch < 1 {
		prefetch = 1
	}
	return min(prefetch, expect)
}

// closeErr convierte la notificación de cierre del canal en un error (ce es nil si el cierre fue limpio).
func closeErr(ce *amqp.Error) error {
	if ce == nil {
		return errors.New("canal cerrado")
	}
	return fmt.Errorf("canal cerrado: %w", ce)
}
