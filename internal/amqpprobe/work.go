package amqpprobe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// WorkOptions configura el consumidor continuo.
type WorkOptions struct {
	Queue    string
	Prefetch int
	// Delay simula el tiempo de proceso por mensaje; con un solo worker, la capacidad es 1/Delay msgs/s.
	Delay time.Duration
}

const (
	reconnectMin = time.Second
	reconnectMax = 10 * time.Second
)

// Work consume sin parar hasta que ctx se cancele y reconecta si el broker o la red fallan.
func Work(ctx context.Context, cfg Config, vhost string, opts WorkOptions, m *Metrics, log *slog.Logger) error {
	if opts.Queue == "" {
		return errors.New("falta --queue")
	}
	backoff := reconnectMin
	for {
		ready, err := workSession(ctx, cfg, vhost, opts, m, log)
		m.SetConnected(false)
		if ready {
			backoff = reconnectMin
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Warn("sesión AMQP terminada, reintentando", "error", err, "backoff", backoff.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff)
	}
}

// workSession devuelve ready=true si llegó a consumir (para reiniciar el backoff).
func workSession(ctx context.Context, cfg Config, vhost string, opts WorkOptions, m *Metrics, log *slog.Logger) (ready bool, err error) {
	conn, err := cfg.Dial(vhost, "amqp-probe-work")
	if err != nil {
		return false, fmt.Errorf("conectar: %w", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("abrir canal: %w", err)
	}
	if err := ch.Qos(max(opts.Prefetch, 1), 0, false); err != nil {
		return false, fmt.Errorf("qos: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	deliveries, err := ch.ConsumeWithContext(ctx, opts.Queue, "", false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("consumir %q: %w", opts.Queue, err)
	}
	m.SetConnected(true)
	log.Info("consumiendo", "vhost", vhost, "queue", opts.Queue, "prefetch", opts.Prefetch, "delay", opts.Delay.String())

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case ce := <-closed:
			return true, closeErr(ce)
		case d, ok := <-deliveries:
			if !ok {
				return true, errors.New("consumidor cancelado")
			}
			if err := process(ctx, d, opts.Delay, m); err != nil {
				return true, err
			}
		}
	}
}

// process simula el trabajo (Delay) y confirma el mensaje. Si ctx se cancela durante el trabajo,
// no hace ack: el broker reentrega el mensaje a otra réplica.
func process(ctx context.Context, d amqp.Delivery, delay time.Duration, m *Metrics) error {
	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	if err := d.Ack(false); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	m.IncConsumed()
	return nil
}

// nextBackoff duplica la espera hasta reconnectMax.
func nextBackoff(d time.Duration) time.Duration {
	return min(d*2, reconnectMax)
}
