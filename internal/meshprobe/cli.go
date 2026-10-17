package meshprobe

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"
)

const usage = `uso: mesh-probe <publish|subscribe> [flags]
  publish   --subject S --count N --msg-id-prefix P [--repeat R] [--ack-timeout D] [--retry-timeout D]
  subscribe --durable NAME --expect N [--stream S] [--timeout D] [--linger D] | --core --subject S
Entorno: NATS_URL, NATS_USER, NATS_PASSWORD`

// Run ejecuta el CLI y devuelve el código de salida. Los resultados salen como línea
// JSON de slog con msg="result".
func Run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) int {
	log := slog.New(slog.NewJSONHandler(out, nil))
	if len(args) == 0 {
		_, _ = fmt.Fprintln(out, usage)
		return 2
	}
	cmd, rest := args[0], args[1:]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		pub PublishOptions
		sub SubscribeOptions
	)
	switch cmd {
	case "publish":
		fs.StringVar(&pub.Subject, "subject", "", "subject destino")
		fs.IntVar(&pub.Count, "count", 1, "mensajes por pasada")
		fs.StringVar(&pub.MsgIDPrefix, "msg-id-prefix", "", "prefijo de Nats-Msg-Id")
		fs.IntVar(&pub.Repeat, "repeat", 1, "veces que se publica cada id")
		fs.DurationVar(&pub.AckTimeout, "ack-timeout", 5*time.Second, "espera del PubAck por intento")
		fs.DurationVar(&pub.RetryTimeout, "retry-timeout", 0, "reintentar cada mensaje hasta este tiempo")
	case "subscribe":
		fs.StringVar(&sub.Stream, "stream", "BILLING_MATCHED", "stream del consumer")
		fs.StringVar(&sub.Durable, "durable", "", "nombre del durable consumer")
		fs.StringVar(&sub.Subject, "subject", "energy.matched.>", "subject (solo --core)")
		fs.BoolVar(&sub.Core, "core", false, "suscripción NATS simple, sin JetStream")
		fs.IntVar(&sub.Expect, "expect", 0, "mensajes esperados (0 = esperar --timeout)")
		fs.DurationVar(&sub.Timeout, "timeout", 30*time.Second, "tope total")
		fs.DurationVar(&sub.Linger, "linger", time.Second, "espera extra tras alcanzar --expect")
	default:
		_, _ = fmt.Fprintln(out, usage)
		return 2
	}
	if err := fs.Parse(rest); err != nil {
		log.Error("flags inválidos", "error", err.Error())
		return 2
	}
	if err := validate(cmd, pub, sub); err != nil {
		log.Error("flags inválidos", "error", err.Error())
		return 2
	}
	conn, err := ConnFromEnv(getenv)
	if err != nil {
		log.Error("configuración inválida", "error", err.Error())
		return 2
	}
	cl, err := Connect(conn)
	if err != nil {
		log.Error("conexión fallida", "error", err.Error())
		return 1
	}
	defer cl.Close()

	switch cmd {
	case "publish":
		log.Info("result", "command", cmd, "user", conn.User, "result", cl.Publish(ctx, pub))
	case "subscribe":
		log.Info("result", "command", cmd, "user", conn.User, "result", cl.Subscribe(ctx, sub))
	}
	return 0
}

func validate(cmd string, pub PublishOptions, sub SubscribeOptions) error {
	switch cmd {
	case "publish":
		if pub.Subject == "" || pub.MsgIDPrefix == "" {
			return fmt.Errorf("--subject y --msg-id-prefix son obligatorios")
		}
		if pub.Count < 1 || pub.Repeat < 1 {
			return fmt.Errorf("--count y --repeat deben ser >= 1")
		}
	case "subscribe":
		if !sub.Core && sub.Durable == "" {
			return fmt.Errorf("--durable es obligatorio (o usa --core)")
		}
		if sub.Expect < 0 {
			return fmt.Errorf("--expect debe ser >= 0")
		}
	}
	return nil
}
