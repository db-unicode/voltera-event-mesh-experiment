package metersim

import (
	"bufio"
	"cmp"
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/w3c"
)

// ServiceName y tipo de lo publicado (aún no es un evento de dominio).
const (
	ServiceName = "meter-sim"
	EventType   = "MeterDeclaration"
)

// LateThreshold: una declaración es "late" si sale más de este tiempo después de su turno.
const LateThreshold = 100 * time.Millisecond

// DefaultQueueDepth es el búfer entre el calendario y los publicadores: si se llena, la
// declaración se descarta ("dropped") en vez de retrasar el calendario (modelo abierto).
const DefaultQueueDepth = 4096

// Sender publica y vuelve tras el confirm (*amqpx.Publisher).
type Sender interface {
	Send(ctx context.Context, o amqpx.Outgoing) error
}

// Summary es el resultado de la corrida (JSON en stdout al terminar).
type Summary struct {
	Profile       Profile        `json:"profile"`
	Region        string         `json:"region"`
	Seed          int64          `json:"seed"`
	Meters        int            `json:"meters"`
	Rate          float64        `json:"rate"`
	Planned       int            `json:"planned"`
	Scheduled     int64          `json:"scheduled"`
	Published     int64          `json:"published"`
	Errors        int64          `json:"errors"`
	Late          int64          `json:"late"`
	Dropped       int64          `json:"dropped"`
	MaxLatenessMs float64        `json:"maxLatenessMs"`
	ByVendor      map[string]int `json:"byVendor"`
	ExpectedPairs int            `json:"expectedPairs,omitempty"`
	StartedAt     time.Time      `json:"startedAt"`
	EndedAt       time.Time      `json:"endedAt"`
	ElapsedSec    float64        `json:"elapsedSec"`
	AchievedRate  float64        `json:"achievedRate"`
	CSV           string         `json:"csv,omitempty"`
}

// DirectChannel es el canal al que publica direct-positions (salta el gateway).
const DirectChannel = "energy-positions"

// outgoing arma el mensaje de d: una declaración de fabricante en meter-declarations o, en direct-positions, un
// PositionDeclared con el estado completo en energy-positions (eventId = el de la declaración; committedAt y
// emittedAt = instante de envío, como si el gateway ya hubiera confirmado).
func (o Options) outgoing(d Declaration, sentAt time.Time) (amqpx.Outgoing, error) {
	if o.Profile == ProfileDirectPositions {
		env, err := events.NewPositionDeclared(o.Region, d.Version, sentAt, events.PositionDeclared{
			MeterID: d.MeterID, Side: d.Side, KWh: d.KWh, ValidUntil: sentAt.Add(d.Horizon).UTC(), Zone: "z-" + strconv.Itoa(d.Zone)})
		if err != nil {
			return amqpx.Outgoing{}, err
		}
		env.EventID = d.EventID
		env.CommittedAt, env.EmittedAt, env.Traceparent = sentAt.UTC(), sentAt.UTC(), w3c.NewTraceparent()
		body, err := events.Marshal(env)
		if err != nil {
			return amqpx.Outgoing{}, err
		}
		out := amqpx.EnvelopeOutgoing(DirectChannel, env, body)
		out.Timestamp = sentAt
		return out, nil
	}
	body, err := d.Encode(sentAt)
	if err != nil {
		return amqpx.Outgoing{}, err
	}
	return amqpx.Outgoing{
		Exchange: Exchange, RoutingKey: QueueName, Body: body, MessageID: d.EventID, Type: EventType, Timestamp: sentAt,
		Headers: map[string]any{"x-vendor": d.Vendor, "traceparent": w3c.NewTraceparent(), "eventId": d.EventID},
	}, nil
}

// publishedLabels son (event_type, channel) de events_published_total.
func (o Options) publishedLabels() (string, string) {
	if o.Profile == ProfileDirectPositions {
		return events.TypePositionDeclared, DirectChannel
	}
	return EventType, QueueName
}

// Runner ejecuta una corrida.
type Runner struct {
	Opts   Options
	Sender Sender
	Svc    *telemetry.Service
	Log    *slog.Logger
	Clock  func() time.Time
	// QueueDepth sustituye a DefaultQueueDepth si es > 0.
	QueueDepth int
}

type job struct {
	idx    int
	target time.Time
}

type metrics struct {
	late, dropped, errors prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		late:    prometheus.NewCounter(prometheus.CounterOpts{Name: "meter_sim_late_total", Help: "Declaraciones enviadas más de 100 ms después de su turno."}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{Name: "meter_sim_dropped_total", Help: "Declaraciones descartadas por cola de envío llena."}),
		errors:  prometheus.NewCounter(prometheus.CounterOpts{Name: "meter_sim_publish_errors_total", Help: "Publicaciones no confirmadas por el broker."}),
	}
	reg.MustRegister(m.late, m.dropped, m.errors)
	return m
}

func (r *Runner) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

// Run publica el plan completo (o hasta que ctx se cancele) y devuelve el resumen.
func (r *Runner) Run(ctx context.Context) (Summary, error) {
	o := r.Opts
	plan, curve, err := NewPlan(o)
	if err != nil {
		return Summary{}, err
	}
	m := newMetrics(r.Svc.Reg)

	var csvw *csv.Writer
	var csvFile *os.File
	var csvMu sync.Mutex
	if o.Out != "" {
		if csvFile, err = os.Create(o.Out); err != nil {
			return Summary{}, fmt.Errorf("metersim: create csv: %w", err)
		}
		csvw = csv.NewWriter(bufio.NewWriterSize(csvFile, 1<<20))
		_ = csvw.Write([]string{"eventId", "meterId", "version", "sentAt"})
	}

	sum := Summary{Profile: o.Profile, Region: o.Region, Seed: o.Seed, Meters: o.Meters, Rate: o.Rate, Planned: plan.Total(),
		ExpectedPairs: plan.ExpectedPairs(), ByVendor: map[string]int{}, CSV: o.Out}
	var scheduled, published, errCount, late, dropped, maxLateNs atomic.Int64
	var vendorMu sync.Mutex

	jobs := make(chan job, cmp.Or(r.QueueDepth, DefaultQueueDepth))
	var wg sync.WaitGroup
	for range o.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				d := plan.At(j.idx)
				sentAt := r.now()
				lateness := sentAt.Sub(j.target)
				if lateness > LateThreshold {
					late.Add(1)
					m.late.Inc()
				}
				for {
					cur := maxLateNs.Load()
					if int64(lateness) <= cur || maxLateNs.CompareAndSwap(cur, int64(lateness)) {
						break
					}
				}
				out, err := o.outgoing(d, sentAt)
				if err == nil {
					err = r.Sender.Send(ctx, out)
				}
				if err != nil {
					errCount.Add(1)
					m.errors.Inc()
					if ctx.Err() == nil && errCount.Load() <= 5 {
						r.Log.Warn("publish failed", "stage", "publish", "result", "error", "meterId", d.MeterID, "error", err)
					}
					continue
				}
				published.Add(1)
				r.Svc.Metrics.EventsPublished.WithLabelValues(o.publishedLabels()).Inc()
				vendorMu.Lock()
				sum.ByVendor[d.Vendor]++
				vendorMu.Unlock()
				if csvw != nil {
					csvMu.Lock()
					_ = csvw.Write([]string{d.EventID, d.MeterID, strconv.FormatInt(d.Version, 10), sentAt.UTC().Format(time.RFC3339Nano)})
					csvMu.Unlock()
				}
			}
		}()
	}

	start := r.now()
	sum.StartedAt = start.UTC()
	r.schedule(ctx, plan, curve, start, jobs, &scheduled, &dropped, m)
	close(jobs)
	wg.Wait()
	end := r.now()

	sum.EndedAt = end.UTC()
	sum.Scheduled, sum.Published, sum.Errors = scheduled.Load(), published.Load(), errCount.Load()
	sum.Late, sum.Dropped = late.Load(), dropped.Load()
	sum.MaxLatenessMs = float64(maxLateNs.Load()) / float64(time.Millisecond)
	sum.ElapsedSec = end.Sub(start).Seconds()
	if sum.ElapsedSec > 0 {
		sum.AchievedRate = float64(sum.Published) / sum.ElapsedSec
	}
	if csvw != nil {
		csvw.Flush()
		if err := csvw.Error(); err != nil {
			return sum, fmt.Errorf("metersim: write csv: %w", err)
		}
		if err := csvFile.Close(); err != nil {
			return sum, fmt.Errorf("metersim: close csv: %w", err)
		}
	}
	return sum, nil
}

// schedule encola cada declaración en su instante. Es el "ticker" del modelo abierto:
// no espera a los publicadores; si la cola está llena descarta (salvo sin límite de tasa,
// donde bloquea porque no hay calendario que respetar).
func (r *Runner) schedule(ctx context.Context, plan Plan, curve Curve, start time.Time, jobs chan<- job,
	scheduled, dropped *atomic.Int64, m *metrics) {
	unthrottled := curve.Rate <= 0
	for i := range plan.Total() {
		target := start.Add(curve.Time(i + 1))
		if wait := target.Sub(r.now()); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		if ctx.Err() != nil {
			return
		}
		j := job{idx: i, target: target}
		if unthrottled {
			select {
			case jobs <- j:
				scheduled.Add(1)
			case <-ctx.Done():
				return
			}
			continue
		}
		select {
		case jobs <- j:
			scheduled.Add(1)
		default:
			dropped.Add(1)
			m.dropped.Inc()
		}
	}
}

// VendorCounts devuelve ByVendor como "vendor=n" ordenado (para logs).
func (s Summary) VendorCounts() []string {
	keys := make([]string, 0, len(s.ByVendor))
	for k := range s.ByVendor {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+strconv.Itoa(s.ByVendor[k]))
	}
	return out
}
