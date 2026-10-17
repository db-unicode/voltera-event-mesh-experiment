package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Record es un mensaje entregado al Handler.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Handler procesa un mensaje. nil ⇒ el Consumer confirma el offset; error ⇒ no se
// confirma, el Consumer espera (backoff) y reintenta el MISMO mensaje. El ctx que
// recibe no se cancela con el apagado: el mensaje en curso termina; el handler debe
// acotar sus propias esperas con timeouts.
type Handler func(ctx context.Context, rec Record) error

// ConsumerConfig configura el Consumer.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	Group    string
	Topic    string
	// InitialBackoff y MaxBackoff acotan la espera entre reintentos de un handler
	// fallido (por defecto 200 ms y 5 s, duplicando).
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// PollRecords es el tamaño máximo de cada poll (por defecto 100).
	PollRecords int
	Logger      *slog.Logger
	Registerer  prometheus.Registerer
	// Opts se añaden al final (tests, ajustes).
	Opts []kgo.Opt
}

// polled es la parte de *kgo.Client que usa el bucle (se sustituye en los tests).
type polled interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
	Ping(ctx context.Context) error
	Close()
}

// Consumer lee un topic como miembro de un grupo. Procesa cada partición en orden y
// confirma el offset de cada mensaje solo después de que el handler devolvió nil.
// Se escala añadiendo réplicas (hasta el número de particiones).
type Consumer struct {
	cl      polled
	cfg     ConsumerConfig
	log     *slog.Logger
	handled *prometheus.CounterVec
	seconds *prometheus.HistogramVec
}

// NewConsumer crea el cliente de grupo. Un grupo nuevo empieza en el inicio del log
// (earliest); el auto-commit está desactivado.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Group == "" || cfg.Topic == "" {
		return nil, errors.New("kafkax: brokers, group and topic are required")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
	cl, err := kgo.NewClient(append(opts, cfg.Opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: new consumer: %w", err)
	}
	return newConsumer(cl, cfg), nil
}

func newConsumer(cl polled, cfg ConsumerConfig) *Consumer {
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 200 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = max(5*time.Second, cfg.InitialBackoff)
	}
	if cfg.PollRecords <= 0 {
		cfg.PollRecords = 100
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Consumer{
		cl:  cl,
		cfg: cfg,
		log: cfg.Logger,
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consume_handled_total", Help: "Mensajes procesados por el handler del consumidor.",
			ConstLabels: prometheus.Labels{"group": cfg.Group, "topic": cfg.Topic}}, []string{"result"}),
		seconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kafka_consume_handle_seconds", Help: "Duración del handler por mensaje.",
			ConstLabels: prometheus.Labels{"group": cfg.Group, "topic": cfg.Topic},
			Buckets:     []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}}, []string{"result"}),
	}
	c.handled.WithLabelValues("ok")
	c.handled.WithLabelValues("error")
	if cfg.Registerer != nil {
		cfg.Registerer.MustRegister(c.handled, c.seconds)
	}
	return c
}

// Run consume hasta que ctx se cancele (devuelve nil) o el cliente se cierre.
// Los errores transitorios del broker se registran y se siguen reintentando.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for ctx.Err() == nil {
		fetches := c.cl.PollRecords(ctx, c.cfg.PollRecords)
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
			break
		}
		if fetches.IsClientClosed() {
			return errors.New("kafkax: client closed")
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("kafka fetch error", "stage", "consume", "result", "error",
				"topic", topic, "partition", partition, "error", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				if ctx.Err() != nil || !c.process(ctx, h, r) {
					return // apagado a mitad de un reintento: lo no confirmado se reentrega
				}
			}
		})
		c.cl.AllowRebalance()
	}
	return nil
}

// process ejecuta el handler con reintentos hasta éxito y confirma el offset.
// Devuelve false si ctx se canceló antes de poder confirmar.
func (c *Consumer) process(ctx context.Context, h Handler, r *kgo.Record) bool {
	rec := toRecord(r)
	backoff := c.cfg.InitialBackoff
	for {
		start := time.Now()
		err := h(context.WithoutCancel(ctx), rec)
		if err == nil {
			c.observe("ok", start)
			c.commit(ctx, r)
			return true
		}
		c.observe("error", start)
		c.log.Warn("handler failed; retrying the same message", "stage", "consume", "result", "error",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "backoff", backoff.String(), "error", err)
		if !sleep(ctx, backoff) {
			return false
		}
		backoff = min(backoff*2, c.cfg.MaxBackoff)
	}
}

// commit confirma el offset del mensaje. Un fallo no es fatal: el mensaje se
// reentregará y el handler es idempotente.
func (c *Consumer) commit(ctx context.Context, r *kgo.Record) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(cctx, r); err != nil {
		c.log.Warn("offset commit failed; the message may be redelivered", "stage", "commit", "result", "error",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
	}
}

func (c *Consumer) observe(result string, start time.Time) {
	c.handled.WithLabelValues(result).Inc()
	c.seconds.WithLabelValues(result).Observe(time.Since(start).Seconds())
}

// Ping comprueba que hay al menos un broker alcanzable (readyz).
func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// Close sale del grupo y cierra el cliente.
func (c *Consumer) Close() { c.cl.Close() }

func toRecord(r *kgo.Record) Record {
	hdrs := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		hdrs[h.Key] = string(h.Value)
	}
	return Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
		Headers: hdrs, Timestamp: r.Timestamp}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
