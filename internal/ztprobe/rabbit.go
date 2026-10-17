package ztprobe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// unroutableKey no está ligada a ninguna cola: un publish permitido se confirma y el mensaje se descarta, sin
// efecto en los consumidores reales. El permiso de escritura se evalúa sobre el exchange.
const unroutableKey = "zt-probe-unroutable"

// rabbit: target = vhost:cola. publish va al exchange "<cola>.x"; consume abre un consumidor sobre la cola.
func (r *Runner) rabbit(ctx context.Context, c Cell) error {
	vhost, queue, ok := strings.Cut(c.Target, ":")
	if !ok || vhost == "" || queue == "" {
		return fmt.Errorf("target %q: se espera vhost:cola", c.Target)
	}
	uri := amqp.URI{Scheme: "amqp", Host: r.Env.AMQPHost, Port: r.Env.AMQPPort,
		Username: r.Env.AMQPUser, Password: r.Env.AMQPPassword, Vhost: vhost}.String()
	conn, err := amqp.DialConfig(uri, amqp.Config{
		Dial:       amqp.DefaultDial(min(5*time.Second, remaining(ctx))),
		Properties: amqp.Table{"connection_name": "zt-probe-" + c.ID},
	})
	if err != nil {
		return fmt.Errorf("conectar: %w", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("abrir canal: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	switch c.Op {
	case OpPublish:
		if err := ch.Confirm(false); err != nil {
			return err
		}
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, queue+".x", unroutableKey, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent, Body: []byte(`{"zt-probe":true}`), MessageId: "zt-" + r.RunID + "-" + c.ID,
		})
		if err != nil {
			return fmt.Errorf("publicar: %w", err)
		}
		// un 403 llega como cierre del canal, no como error del publish
		done := make(chan bool, 1)
		go func() {
			ok, _ := dc.WaitContext(ctx)
			done <- ok
		}()
		select {
		case ce := <-closed:
			return closeErr(ce)
		case ok := <-done:
			if !ok {
				select {
				case ce := <-closed:
					return closeErr(ce)
				default:
				}
				return errors.New("publish sin confirm")
			}
			return nil
		case <-ctx.Done():
			return fmt.Errorf("publish sin confirm: %w", ctx.Err())
		}
	case OpConsume:
		// prefetch 1 y devolución inmediata: como mucho se toca (y se reencola) un mensaje real
		if err := ch.Qos(1, 0, false); err != nil {
			return err
		}
		deliveries, err := ch.ConsumeWithContext(ctx, queue, "zt-probe-"+c.ID, false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("consumir: %w", err)
		}
		select {
		case ce := <-closed:
			return closeErr(ce)
		case d := <-deliveries:
			_ = d.Reject(true)
			return nil
		case <-time.After(1500 * time.Millisecond):
			// el broker aceptó el consumidor y la cola estaba vacía
			select {
			case ce := <-closed:
				return closeErr(ce)
			default:
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("operación %q no soportada", c.Op)
}

func closeErr(e *amqp.Error) error {
	if e == nil {
		return errors.New("canal cerrado")
	}
	return e
}

// remaining devuelve el tiempo que queda del contexto (1 s si no tiene plazo).
func remaining(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return max(time.Until(dl), 100*time.Millisecond)
	}
	return time.Second
}
