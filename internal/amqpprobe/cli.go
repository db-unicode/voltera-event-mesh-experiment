package amqpprobe

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

const usage = `uso: amqp-probe <publish|consume|work> [flags]

  publish --vhost V --queue Q [--exchange X] [--count N] [--rate MSGS_POR_S]
          publica N mensajes persistentes con confirms. Sin --exchange usa "<Q>.x"; --exchange="" usa el exchange por defecto.
  consume --vhost V --queue Q --expect N [--prefetch P] [--timeout D]
          consume N mensajes con ack manual y termina.
  work    --vhost V --queue Q [--prefetch P] [--delay D] [--listen :8080]
          consumidor continuo; sirve /metrics (events_consumed_total), /healthz y /readyz.

Entorno: AMQP_HOST, AMQP_PORT, AMQP_USER, AMQP_PASSWORD, AMQP_VHOST (el flag --vhost manda).
`

// Run ejecuta un subcomando y devuelve el código de salida: 0 ok, 1 fallo, 2 uso incorrecto.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	cfg, err := LoadConfig(getenv)
	if err != nil {
		log.Error("configuración inválida", "error", err)
		return 2
	}

	var runErr error
	switch args[0] {
	case "publish":
		runErr = runPublish(ctx, cfg, args[1:], stdout, stderr)
	case "consume":
		runErr = runConsume(ctx, cfg, args[1:], stdout, stderr)
	case "work":
		runErr = runWork(ctx, cfg, args[1:], stderr, log)
	case "-h", "--help", "help":
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "subcomando desconocido %q\n%s", args[0], usage)
		return 2
	}
	switch {
	case runErr == nil:
		return 0
	case errors.Is(runErr, flag.ErrHelp), errors.Is(runErr, errUsage):
		return 2
	default:
		log.Error("falló", "cmd", args[0], "error", runErr)
		return 1
	}
}

var errUsage = errors.New("uso incorrecto")

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

// publishArgs son los flags ya validados de `publish`.
type publishArgs struct {
	vhost   string
	opts    PublishOptions
	timeout time.Duration
}

func parsePublish(cfg Config, args []string, stderr io.Writer) (publishArgs, error) {
	fs := newFlagSet("publish", stderr)
	vhost := fs.String("vhost", cfg.Vhost, "vhost")
	exchange := fs.String("exchange", "", `exchange (por defecto "<queue>.x")`)
	queue := fs.String("queue", "", "cola destino (routing key)")
	count := fs.Int("count", 1, "mensajes a publicar")
	rate := fs.Int("rate", 0, "mensajes por segundo (0 = sin límite)")
	timeout := fs.Duration("timeout", 2*time.Minute, "tiempo máximo")
	if err := fs.Parse(args); err != nil {
		return publishArgs{}, err
	}
	exchangeSet := false
	fs.Visit(func(f *flag.Flag) { exchangeSet = exchangeSet || f.Name == "exchange" })
	ex, key, err := resolveTarget(*exchange, exchangeSet, *queue)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return publishArgs{}, errUsage
	}
	if *vhost == "" || *count < 1 || *rate < 0 {
		_, _ = fmt.Fprintln(stderr, "faltan --vhost, o --count < 1, o --rate < 0")
		return publishArgs{}, errUsage
	}
	return publishArgs{vhost: *vhost, timeout: *timeout, opts: PublishOptions{Exchange: ex, RoutingKey: key, Count: *count, Rate: *rate}}, nil
}

func runPublish(ctx context.Context, cfg Config, args []string, stdout, stderr io.Writer) error {
	a, err := parsePublish(cfg, args, stderr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	conn, err := cfg.Dial(a.vhost, "amqp-probe-publish")
	if err != nil {
		return fmt.Errorf("conectar: %w", err)
	}
	defer func() { _ = conn.Close() }()
	res, err := Publish(ctx, conn, a.opts)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(res); err != nil {
		return err
	}
	if !res.OK(a.opts.Count) {
		return fmt.Errorf("publicación incompleta: %+v", res)
	}
	return nil
}

// consumeArgs son los flags ya validados de `consume`.
type consumeArgs struct {
	vhost   string
	opts    ConsumeOptions
	timeout time.Duration
}

func parseConsume(cfg Config, args []string, stderr io.Writer) (consumeArgs, error) {
	fs := newFlagSet("consume", stderr)
	vhost := fs.String("vhost", cfg.Vhost, "vhost")
	queue := fs.String("queue", "", "cola a consumir")
	expect := fs.Int("expect", 0, "mensajes a consumir")
	prefetch := fs.Int("prefetch", 10, "prefetch (qos)")
	timeout := fs.Duration("timeout", time.Minute, "tiempo máximo")
	if err := fs.Parse(args); err != nil {
		return consumeArgs{}, err
	}
	if *vhost == "" || *queue == "" || *expect < 1 {
		_, _ = fmt.Fprintln(stderr, "faltan --vhost, --queue o --expect >= 1")
		return consumeArgs{}, errUsage
	}
	return consumeArgs{vhost: *vhost, timeout: *timeout, opts: ConsumeOptions{Queue: *queue, Expect: *expect, Prefetch: *prefetch}}, nil
}

func runConsume(ctx context.Context, cfg Config, args []string, stdout, stderr io.Writer) error {
	a, err := parseConsume(cfg, args, stderr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	conn, err := cfg.Dial(a.vhost, "amqp-probe-consume")
	if err != nil {
		return fmt.Errorf("conectar: %w", err)
	}
	defer func() { _ = conn.Close() }()
	res, err := Consume(ctx, conn, a.opts)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(res)
}

// workArgs son los flags ya validados de `work`.
type workArgs struct {
	vhost  string
	listen string
	opts   WorkOptions
}

func parseWork(cfg Config, args []string, stderr io.Writer) (workArgs, error) {
	fs := newFlagSet("work", stderr)
	vhost := fs.String("vhost", cfg.Vhost, "vhost")
	queue := fs.String("queue", "", "cola a consumir")
	prefetch := fs.Int("prefetch", 10, "prefetch (qos)")
	delay := fs.Duration("delay", 20*time.Millisecond, "tiempo de proceso simulado por mensaje")
	listen := fs.String("listen", ":8080", "dirección de /metrics y /healthz")
	if err := fs.Parse(args); err != nil {
		return workArgs{}, err
	}
	if *vhost == "" || *queue == "" || *prefetch < 1 || *delay < 0 {
		_, _ = fmt.Fprintln(stderr, "faltan --vhost o --queue, o --prefetch < 1, o --delay < 0")
		return workArgs{}, errUsage
	}
	return workArgs{vhost: *vhost, listen: *listen, opts: WorkOptions{Queue: *queue, Prefetch: *prefetch, Delay: *delay}}, nil
}

func runWork(ctx context.Context, cfg Config, args []string, stderr io.Writer, log *slog.Logger) error {
	a, err := parseWork(cfg, args, stderr)
	if err != nil {
		return err
	}
	m := &Metrics{}
	ln, err := net.Listen("tcp", a.listen)
	if err != nil {
		return fmt.Errorf("escuchar %s: %w", a.listen, err)
	}
	srv := &http.Server{Handler: m.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("servidor de métricas", "error", err)
		}
	}()
	log.Info("métricas y healthz", "addr", ln.Addr().String())

	workErr := Work(ctx, cfg, a.vhost, a.opts, m, log)

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("cerrado", "consumed", m.Consumed())
	return workErr
}
