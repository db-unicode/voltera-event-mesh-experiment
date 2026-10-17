package ztprobe

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const usage = `uso: zt-probe [--cells FILE|-] [--timeout 8s] [--parallel 8]
Lee celdas CSV (id,broker,op,target) y escribe un resultado JSON por línea.
Entorno: AMQP_HOST, AMQP_PORT, AMQP_USER, AMQP_PASSWORD; KAFKA_BROKERS; NATS_URL, NATS_USER, NATS_PASSWORD.
Vacío = sin credencial; una celda de un broker sin su entorno se rechaza al conectar.`

// Env son los destinos y credenciales con los que se intentan las celdas.
type Env struct {
	AMQPHost, AMQPUser, AMQPPassword string
	AMQPPort                         int
	KafkaBrokers                     []string
	NATSURL, NATSUser, NATSPassword  string
}

// EnvFrom lee el entorno.
func EnvFrom(getenv func(string) string) (Env, error) {
	e := Env{
		AMQPHost: orDefault(getenv("AMQP_HOST"), "rabbitmq.voltera-infra.svc"), AMQPPort: 5672,
		AMQPUser: getenv("AMQP_USER"), AMQPPassword: getenv("AMQP_PASSWORD"),
		NATSURL: getenv("NATS_URL"), NATSUser: getenv("NATS_USER"), NATSPassword: getenv("NATS_PASSWORD"),
	}
	if p := getenv("AMQP_PORT"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return Env{}, fmt.Errorf("AMQP_PORT inválido: %q", p)
		}
		e.AMQPPort = n
	}
	for _, b := range strings.Split(orDefault(getenv("KAFKA_BROKERS"), "kafka.voltera-infra.svc:9092"), ",") {
		b = strings.TrimSpace(b)
		if _, _, err := net.SplitHostPort(b); err != nil {
			return Env{}, fmt.Errorf("KAFKA_BROKERS: %q inválido: %w", b, err)
		}
		e.KafkaBrokers = append(e.KafkaBrokers, b)
	}
	if e.NATSURL != "" && !strings.HasPrefix(e.NATSURL, "nats://") {
		return Env{}, fmt.Errorf("NATS_URL debe empezar con nats://")
	}
	return e, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// Runner ejecuta celdas.
type Runner struct {
	Env      Env
	Timeout  time.Duration
	Parallel int
	// RunID hace únicos los mensajes de prueba entre corridas.
	RunID string
}

// Run ejecuta todas las celdas (con paralelismo acotado) y devuelve los resultados en el orden de entrada.
func (r *Runner) Run(ctx context.Context, cells []Cell) []Result {
	par := max(r.Parallel, 1)
	out := make([]Result, len(cells))
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	for i, c := range cells {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			out[i] = r.runCell(ctx, c)
		}()
	}
	wg.Wait()
	return out
}

func (r *Runner) runCell(ctx context.Context, c Cell) Result {
	start := time.Now()
	cctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	var err error
	switch c.Broker {
	case BrokerRabbit:
		err = r.rabbit(cctx, c)
	case BrokerKafka:
		err = r.kafka(cctx, c)
	case BrokerNATS:
		err = r.nats(cctx, c)
	default:
		err = fmt.Errorf("broker desconocido %q", c.Broker)
	}
	res := Result{ID: c.ID, Broker: c.Broker, Op: c.Op, Target: c.Target, Outcome: Allowed, Ms: since(start)}
	if err != nil {
		res.Outcome, res.Class, res.Error = Denied, Classify(err), err.Error()
	}
	return res
}

// Run es el punto de entrada del CLI. Devuelve 0 si ejecutó las celdas (aunque se rechacen), 2 por uso.
func Run(ctx context.Context, args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("zt-probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cellsPath := fs.String("cells", "-", "archivo de celdas (- = stdin)")
	timeout := fs.Duration("timeout", 8*time.Second, "tiempo máximo por celda")
	parallel := fs.Int("parallel", 8, "celdas en paralelo")
	if err := fs.Parse(args); err != nil {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	env, err := EnvFrom(getenv)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "configuración inválida:", err)
		return 2
	}
	in := stdin
	if *cellsPath != "-" {
		f, err := os.Open(*cellsPath)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 2
		}
		defer func() { _ = f.Close() }()
		in = f
	}
	cells, err := ParseCells(in)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "celdas inválidas:", err)
		return 2
	}
	r := &Runner{Env: env, Timeout: *timeout, Parallel: *parallel, RunID: strconv.FormatInt(time.Now().UnixNano(), 36)}
	results := r.Run(ctx, cells)
	sort.SliceStable(results, func(i, j int) bool { return results[i].ID < results[j].ID })
	enc := json.NewEncoder(stdout)
	for _, res := range results {
		_ = enc.Encode(res)
	}
	return 0
}
