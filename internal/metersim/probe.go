package metersim

// Perfil probe (E3): mide cuánto tarda una declaración en ser visible en la vista Redis.
//
// Es una herramienta de medición, no parte del dominio: lee la vista `market-view` con las credenciales del Secret
// market-view-credentials porque Redis no tiene usuarios de solo lectura. No escribe nunca en Redis.
// Cada sondeo declara un medidor probe-* con una versión que no se repite; cuando el broker confirma la
// publicación, lee `_v` de la key del medidor cada --poll hasta ver esa versión y anota visible_at.
// La resolución es --poll (20 ms) más el RTT de Redis. Con --read-rate genera además una carga de lectura de fondo.

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/market/projection"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// ProbePrefix es el prefijo obligatorio de los medidores del perfil probe (E3 los localiza con LIKE 'probe-%').
const ProbePrefix = "probe-"

// viewVersionField es el campo donde view.Store guarda la versión (`_v`, reservado en internal/view).
const viewVersionField = "_v"

// Estados de un sondeo.
const (
	ProbeOK      = "ok"
	ProbeTimeout = "timeout"
	ProbeError   = "error" // publicación fallida o Redis inaccesible durante todo el sondeo
)

// ViewClient es el acceso de solo lectura a la vista Redis.
type ViewClient interface {
	// Version devuelve la versión guardada en key; ok=false si la key no existe.
	Version(ctx context.Context, key string) (v int64, ok bool, err error)
	// Keys devuelve las keys que coinciden con pattern (SCAN).
	Keys(ctx context.Context, pattern string) ([]string, error)
	// Get lee la key completa (HGETALL), como hace el matching-engine.
	Get(ctx context.Context, key string) error
}

type redisView struct{ rdb redis.UniversalClient }

// NewRedisView envuelve un cliente go-redis como ViewClient.
func NewRedisView(rdb redis.UniversalClient) ViewClient { return redisView{rdb: rdb} }

func (r redisView) Version(ctx context.Context, key string) (int64, bool, error) {
	s, err := r.rdb.HGet(ctx, key, viewVersionField).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("metersim: %s has invalid version %q", key, s)
	}
	return v, true, nil
}

func (r redisView) Keys(ctx context.Context, pattern string) ([]string, error) {
	var out []string
	var cursor uint64
	for {
		batch, next, err := r.rdb.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

func (r redisView) Get(ctx context.Context, key string) error {
	return r.rdb.HGetAll(ctx, key).Err()
}

// ProbeResult es el resultado de un sondeo.
type ProbeResult struct {
	EventID     string
	MeterID     string
	Version     int64
	SentAt      time.Time // antes de publicar
	ConfirmedAt time.Time // confirm del broker (cero si falló la publicación)
	VisibleAt   time.Time // primera lectura que vio la versión (cero si no es ok)
	Status      string
}

// ProbeRunner ejecuta el perfil probe.
type ProbeRunner struct {
	Opts   Options
	Sender Sender
	View   ViewClient
	Svc    *telemetry.Service
	Log    *slog.Logger
	Stdout io.Writer // destino de los CSV "-" y del resumen
	Clock  func() time.Time
}

func (p *ProbeRunner) now() time.Time {
	if p.Clock != nil {
		return p.Clock()
	}
	return time.Now()
}

// ProbeSummary es el resumen JSON (empieza por "profile" como el de Runner).
type ProbeSummary struct {
	Profile         Profile      `json:"profile"`
	Region          string       `json:"region"`
	Prefix          string       `json:"prefix"`
	Meters          int          `json:"meters"`
	Rate            float64      `json:"rate"`
	Planned         int          `json:"planned"`
	Sent            int64        `json:"sent"`
	Visible         int64        `json:"visible"`
	Timeouts        int64        `json:"timeouts"`
	Errors          int64        `json:"errors"`
	PollErrors      int64        `json:"pollErrors"`
	SentToVisibleMs Quantiles    `json:"sentToVisibleMs"`
	Reads           *ReadSummary `json:"reads,omitempty"`
	StartedAt       time.Time    `json:"startedAt"`
	EndedAt         time.Time    `json:"endedAt"`
	ElapsedSec      float64      `json:"elapsedSec"`
	CSV             string       `json:"csv,omitempty"`
	ReadCSV         string       `json:"readCsv,omitempty"`
}

// Quantiles son percentiles en las unidades de la serie.
type Quantiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func quantilesOf(vals []float64) Quantiles {
	if len(vals) == 0 {
		return Quantiles{}
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	at := func(q float64) float64 { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return Quantiles{P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: s[len(s)-1]}
}

// Run publica los sondeos (uno por turno, cada uno en su goroutine) y, si hay --read-rate, lee en paralelo.
// Termina cuando todos los sondeos se resolvieron (visibles, timeout o error) y la duración transcurrió.
func (p *ProbeRunner) Run(ctx context.Context) (ProbeSummary, error) {
	o := p.Opts
	plan, curve, err := NewPlan(o)
	if err != nil {
		return ProbeSummary{}, err
	}
	sum := ProbeSummary{Profile: o.Profile, Region: o.Region, Prefix: o.Prefix, Meters: o.Meters, Rate: o.Rate,
		Planned: plan.Total(), CSV: o.Out, ReadCSV: o.ReadOut}

	var (
		mu       sync.Mutex
		results  []ProbeResult
		wg       sync.WaitGroup
		pollErrs atomic.Int64
	)
	start := p.now()
	sum.StartedAt = start.UTC()

	var rl *ReadLoad
	readsDone := make(chan struct{})
	if o.ReadRate > 0 {
		rl = &ReadLoad{View: p.View, Rate: o.ReadRate, Log: p.Log, Clock: p.Clock}
		readCtx, cancel := context.WithTimeout(ctx, o.Duration)
		defer cancel()
		go func() { defer close(readsDone); rl.Run(readCtx) }()
	} else {
		close(readsDone)
	}

	record := func(r ProbeResult) {
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	}
	for i := range plan.Total() {
		if wait := start.Add(curve.Time(i + 1)).Sub(p.now()); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
			case <-t.C:
			}
		}
		if ctx.Err() != nil {
			break
		}
		d := plan.At(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			record(p.probe(ctx, d, &pollErrs))
		}()
	}
	wg.Wait()
	<-readsDone // la carga de lectura dura --duration aunque no haya sondeos
	end := p.now()
	sum.EndedAt, sum.ElapsedSec = end.UTC(), end.Sub(start).Seconds()

	sort.Slice(results, func(a, b int) bool { return results[a].SentAt.Before(results[b].SentAt) })
	var lat []float64
	for _, r := range results {
		switch r.Status {
		case ProbeOK:
			sum.Visible++
			lat = append(lat, float64(r.VisibleAt.Sub(r.SentAt))/float64(time.Millisecond))
		case ProbeTimeout:
			sum.Timeouts++
		default:
			sum.Errors++
		}
		if !r.ConfirmedAt.IsZero() {
			sum.Sent++
		}
	}
	sum.PollErrors = pollErrs.Load()
	sum.SentToVisibleMs = quantilesOf(lat)
	if rl != nil {
		rs := rl.Summary()
		sum.Reads = &rs
	}

	if o.Out != "" {
		if err := p.writeRows(o.Out, "PROBE", probeRows(results)); err != nil {
			return sum, err
		}
	}
	if rl != nil && o.ReadOut != "" {
		if err := p.writeRows(o.ReadOut, "READS", rl.Rows()); err != nil {
			return sum, err
		}
	}
	return sum, nil
}

// probe publica la declaración d y espera a verla en la vista.
func (p *ProbeRunner) probe(ctx context.Context, d Declaration, pollErrs *atomic.Int64) ProbeResult {
	o := p.Opts
	res := ProbeResult{EventID: d.EventID, MeterID: d.MeterID, Version: d.Version, SentAt: p.now(), Status: ProbeError}
	out, err := o.outgoing(d, res.SentAt)
	if err == nil {
		err = p.Sender.Send(ctx, out)
	}
	if err != nil {
		if ctx.Err() == nil {
			p.Log.Warn("probe publish failed", "stage", "publish", "result", "error", "meterId", d.MeterID, "error", err)
		}
		return res
	}
	res.ConfirmedAt = p.now()
	p.Svc.Metrics.EventsPublished.WithLabelValues(EventType, QueueName).Inc()

	key := projection.PositionKey("z-"+strconv.Itoa(d.Zone), d.MeterID)
	deadline := res.ConfirmedAt.Add(o.ProbeTimeout)
	readOK := false
	for {
		v, found, err := p.View.Version(ctx, key)
		now := p.now()
		switch {
		case err != nil:
			pollErrs.Add(1)
		case found && v >= d.Version:
			res.VisibleAt, res.Status = now, ProbeOK
			return res
		default:
			readOK = true
		}
		if ctx.Err() != nil || now.After(deadline) {
			if readOK {
				res.Status = ProbeTimeout
			}
			return res
		}
		select {
		case <-time.After(o.ProbePoll):
		case <-ctx.Done():
		}
	}
}

func microsOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixMicro(), 10)
}

func stampOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// probeRows devuelve el CSV de sondeos (con cabecera). sent_us/visible_us son epoch en µs: permiten restar con
// enteros exactos en awk contra outbox.created_at (extract(epoch ...)*1e6).
func probeRows(rs []ProbeResult) [][]string {
	rows := [][]string{{"eventId", "meterId", "version", "sent_at", "confirmed_at", "visible_at", "sent_us", "visible_us", "status"}}
	for _, r := range rs {
		rows = append(rows, []string{r.EventID, r.MeterID, strconv.FormatInt(r.Version, 10), stampOrEmpty(r.SentAt),
			stampOrEmpty(r.ConfirmedAt), stampOrEmpty(r.VisibleAt), microsOrEmpty(r.SentAt), microsOrEmpty(r.VisibleAt), r.Status})
	}
	return rows
}

// writeRows escribe rows como CSV en dest; "-" = stdout con cada línea precedida por "<prefix> ".
func (p *ProbeRunner) writeRows(dest, prefix string, rows [][]string) error {
	if dest == "-" {
		var sb strings.Builder
		w := csv.NewWriter(&sb)
		_ = w.WriteAll(rows)
		for _, line := range strings.Split(strings.TrimRight(sb.String(), "\n"), "\n") {
			if _, err := fmt.Fprintf(p.Stdout, "%s %s\n", prefix, line); err != nil {
				return err
			}
		}
		return nil
	}
	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("metersim: create csv: %w", err)
	}
	bw := bufio.NewWriter(f)
	w := csv.NewWriter(bw)
	if err := w.WriteAll(rows); err != nil {
		_ = f.Close()
		return fmt.Errorf("metersim: write csv: %w", err)
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// ---- carga de lectura ----

// ReadSummary resume la carga de lectura.
type ReadSummary struct {
	Rate      float64   `json:"rate"`
	Reads     int64     `json:"reads"`
	Errors    int64     `json:"errors"`
	Skipped   int64     `json:"skipped"`
	LatencyMs Quantiles `json:"latencyMs"`
}

type readSample struct {
	at  time.Time
	lat time.Duration
	err bool
}

// ReadLoad lee la vista a --read-rate lecturas/s (modelo abierto: un turno por lectura, sin esperar a la anterior).
// Cada lectura es un HGETALL de una key pos:* al azar (la lista se refresca por SCAN cada 5 s).
type ReadLoad struct {
	View ViewClient
	Rate float64
	Log  *slog.Logger
	// Clock sustituye a time.Now (tests).
	Clock func() time.Time

	mu      sync.Mutex
	samples []readSample
	skipped atomic.Int64
	keysMu  sync.RWMutex
	keys    []string
}

const (
	readWorkers    = 64
	readKeyRefresh = 5 * time.Second
	readKeyPattern = "pos:*"
	readFallback   = "pos:z-1:__none__"
)

func (l *ReadLoad) now() time.Time {
	if l.Clock != nil {
		return l.Clock()
	}
	return time.Now()
}

func (l *ReadLoad) refreshKeys(ctx context.Context) {
	keys, err := l.View.Keys(ctx, readKeyPattern)
	if err != nil {
		if ctx.Err() == nil {
			l.Log.Warn("read load: key refresh failed", "stage", "read", "result", "error", "error", err)
		}
		return
	}
	l.keysMu.Lock()
	l.keys = keys
	l.keysMu.Unlock()
}

func (l *ReadLoad) pick() string {
	l.keysMu.RLock()
	defer l.keysMu.RUnlock()
	if len(l.keys) == 0 {
		return readFallback
	}
	return l.keys[rand.IntN(len(l.keys))]
}

// Run lee hasta que ctx termine y espera a las lecturas en vuelo.
func (l *ReadLoad) Run(ctx context.Context) {
	l.refreshKeys(ctx)
	go func() {
		t := time.NewTicker(readKeyRefresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				l.refreshKeys(ctx)
			}
		}
	}()
	interval := time.Duration(float64(time.Second) / l.Rate)
	slots := make(chan struct{}, readWorkers)
	var wg sync.WaitGroup
	next := l.now()
	for {
		if wait := next.Sub(l.now()); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
			case <-t.C:
			}
		}
		if ctx.Err() != nil {
			break
		}
		next = next.Add(interval)
		select {
		case slots <- struct{}{}:
		default:
			l.skipped.Add(1)
			continue
		}
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			defer func() { <-slots }()
			t0 := l.now()
			// la lectura en vuelo no se cancela con ctx: se mide completa
			err := l.View.Get(context.WithoutCancel(ctx), key)
			l.mu.Lock()
			l.samples = append(l.samples, readSample{at: t0, lat: l.now().Sub(t0), err: err != nil})
			l.mu.Unlock()
		}(l.pick())
	}
	wg.Wait()
}

// Summary agrega todas las lecturas.
func (l *ReadLoad) Summary() ReadSummary {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := ReadSummary{Rate: l.Rate, Skipped: l.skipped.Load()}
	var lat []float64
	for _, r := range l.samples {
		s.Reads++
		if r.err {
			s.Errors++
			continue
		}
		lat = append(lat, float64(r.lat)/float64(time.Millisecond))
	}
	s.LatencyMs = quantilesOf(lat)
	return s
}

// Rows devuelve el CSV por segundo (con cabecera): ts (inicio del segundo), lecturas, errores y percentiles de las correctas.
func (l *ReadLoad) Rows() [][]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	type bucket struct {
		reads, errs int
		lat         []float64
	}
	by := map[int64]*bucket{}
	for _, r := range l.samples {
		sec := r.at.Unix()
		b := by[sec]
		if b == nil {
			b = &bucket{}
			by[sec] = b
		}
		b.reads++
		if r.err {
			b.errs++
			continue
		}
		b.lat = append(b.lat, float64(r.lat)/float64(time.Millisecond))
	}
	secs := make([]int64, 0, len(by))
	for s := range by {
		secs = append(secs, s)
	}
	sort.Slice(secs, func(a, b int) bool { return secs[a] < secs[b] })
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	rows := [][]string{{"ts", "reads", "errors", "p50_ms", "p95_ms", "p99_ms", "max_ms"}}
	for _, s := range secs {
		b := by[s]
		q := quantilesOf(b.lat)
		rows = append(rows, []string{time.Unix(s, 0).UTC().Format(time.RFC3339Nano), strconv.Itoa(b.reads), strconv.Itoa(b.errs),
			f(q.P50), f(q.P95), f(q.P99), f(q.Max)})
	}
	return rows
}
