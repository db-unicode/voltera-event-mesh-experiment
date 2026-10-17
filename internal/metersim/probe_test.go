package metersim

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// fakeView es una vista en memoria; publish la hace visible tras un retraso.
type fakeView struct {
	mu        sync.Mutex
	versions  map[string]int64
	failFirst int // lecturas de Version que fallan al principio
	calls     atomic.Int64
	getErr    error
	gets      atomic.Int64
	keys      []string
}

func newFakeView() *fakeView { return &fakeView{versions: map[string]int64{}} }

func (f *fakeView) set(key string, v int64) {
	f.mu.Lock()
	f.versions[key] = v
	f.mu.Unlock()
}

func (f *fakeView) Version(_ context.Context, key string) (int64, bool, error) {
	if int(f.calls.Add(1)) <= f.failFirst {
		return 0, false, errors.New("redis down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.versions[key]
	return v, ok, nil
}

func (f *fakeView) Keys(context.Context, string) ([]string, error) { return f.keys, nil }

func (f *fakeView) Get(context.Context, string) error {
	f.gets.Add(1)
	return f.getErr
}

// projectingSender simula gateway+relay+proyector: la versión aparece en la vista `delay` después de publicar.
type projectingSender struct {
	view    *fakeView
	delay   time.Duration
	skip    bool // nunca proyecta (timeout)
	fail    bool
	targets map[string]target
}

type target struct {
	key     string
	version int64
}

func (s *projectingSender) Send(_ context.Context, o amqpx.Outgoing) error {
	if s.fail {
		return errors.New("nack")
	}
	if s.skip {
		return nil
	}
	tg := s.targets[o.MessageID]
	time.AfterFunc(s.delay, func() { s.view.set(tg.key, tg.version) })
	return nil
}

func probeOptions() Options {
	o := testOptions(ProfileProbe)
	o.Prefix, o.Meters, o.Rate, o.Duration = "probe-t-", 5, 50, 200*time.Millisecond
	o.ProbeTimeout, o.ProbePoll, o.VersionBase = 500*time.Millisecond, 5*time.Millisecond, 100
	return o
}

func newProbeRunner(o Options, s Sender, v ViewClient, out *bytes.Buffer) *ProbeRunner {
	svc := telemetry.New(telemetry.Info{Service: ServiceName, Region: o.Region}, slog.New(slog.DiscardHandler))
	return &ProbeRunner{Opts: o, Sender: s, View: v, Svc: svc, Log: slog.New(slog.DiscardHandler), Stdout: out}
}

// targetsFor da la key y versión de cada declaración del plan, por eventId.
func targetsFor(t *testing.T, o Options) map[string]target {
	t.Helper()
	plan, _, err := NewPlan(o)
	require.NoError(t, err)
	m := map[string]target{}
	for i := range plan.Total() {
		d := plan.At(i)
		m[d.EventID] = target{key: "pos:z-" + strconv.Itoa(d.Zone) + ":" + d.MeterID, version: d.Version}
	}
	return m
}

func TestProbeMeasuresTheDelayUntilTheVersionIsVisible(t *testing.T) {
	o := probeOptions()
	view := newFakeView()
	var out bytes.Buffer
	sum, err := newProbeRunner(o, &projectingSender{view: view, delay: 40 * time.Millisecond, targets: targetsFor(t, o)}, view, &out).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10, sum.Planned)
	require.Equal(t, int64(10), sum.Sent)
	require.Equal(t, int64(10), sum.Visible)
	require.Zero(t, sum.Timeouts+sum.Errors)
	require.GreaterOrEqual(t, sum.SentToVisibleMs.P50, 40.0, "no puede verse antes de que exista")
	require.Less(t, sum.SentToVisibleMs.P99, 200.0)
}

func TestProbeTimesOutWhenTheVersionNeverArrives(t *testing.T) {
	o := probeOptions()
	o.Duration, o.ProbeTimeout = 100*time.Millisecond, 60*time.Millisecond
	view := newFakeView()
	sum, err := newProbeRunner(o, &projectingSender{view: view, skip: true, targets: targetsFor(t, o)}, view, &bytes.Buffer{}).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(5), sum.Timeouts)
	require.Zero(t, sum.Visible)
}

func TestProbeCountsPublishFailuresAndToleratesRedisHiccups(t *testing.T) {
	o := probeOptions()
	view := newFakeView()
	sum, err := newProbeRunner(o, &projectingSender{view: view, fail: true, targets: targetsFor(t, o)}, view, &bytes.Buffer{}).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(10), sum.Errors)
	require.Zero(t, sum.Sent)

	view = newFakeView()
	view.failFirst = 3
	sum, err = newProbeRunner(o, &projectingSender{view: view, delay: 10 * time.Millisecond, targets: targetsFor(t, o)}, view, &bytes.Buffer{}).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), sum.PollErrors)
	require.Equal(t, int64(10), sum.Visible, "los errores de lectura se reintentan")
}

func TestProbeCSVToFileAndStdout(t *testing.T) {
	o := probeOptions()
	o.Duration = 100 * time.Millisecond
	view := newFakeView()
	o.Out = filepath.Join(t.TempDir(), "probe.csv")
	var out bytes.Buffer
	_, err := newProbeRunner(o, &projectingSender{view: view, delay: 5 * time.Millisecond, targets: targetsFor(t, o)}, view, &out).Run(context.Background())
	require.NoError(t, err)
	f, err := os.Open(o.Out)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Equal(t, []string{"eventId", "meterId", "version", "sent_at", "confirmed_at", "visible_at", "sent_us", "visible_us", "status"}, rows[0])
	require.Len(t, rows, 6)
	for _, r := range rows[1:] {
		require.True(t, strings.HasPrefix(r[1], "probe-t-"))
		require.Equal(t, ProbeOK, r[8])
		sent, _ := strconv.ParseInt(r[6], 10, 64)
		vis, _ := strconv.ParseInt(r[7], 10, 64)
		require.Greater(t, vis, sent)
	}

	o.Out = "-"
	out.Reset()
	_, err = newProbeRunner(o, &projectingSender{view: view, delay: 5 * time.Millisecond, targets: targetsFor(t, o)}, view, &out).Run(context.Background())
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 6)
	for _, l := range lines {
		require.True(t, strings.HasPrefix(l, "PROBE "), l)
	}
}

func TestProbeVersionsAreUniquePerProbe(t *testing.T) {
	o := probeOptions()
	o.Duration = time.Second
	plan, _, err := NewPlan(o)
	require.NoError(t, err)
	seen := map[string]bool{}
	for i := range plan.Total() {
		d := plan.At(i)
		k := d.MeterID + "@" + strconv.FormatInt(d.Version, 10)
		require.False(t, seen[k], k)
		require.True(t, strings.HasPrefix(d.MeterID, ProbePrefix))
		seen[k] = true
	}
}

func TestReadLoadKeepsTheRateAndAggregatesPerSecond(t *testing.T) {
	view := newFakeView()
	view.keys = []string{"pos:z-1:a", "pos:z-2:b"}
	rl := &ReadLoad{View: view, Rate: 200, Log: slog.New(slog.DiscardHandler)}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	rl.Run(ctx)
	s := rl.Summary()
	require.InDelta(t, 100, float64(s.Reads), 25, "200/s durante 0,5 s")
	require.Zero(t, s.Errors)
	require.Equal(t, s.Reads, view.gets.Load())

	rows := rl.Rows()
	require.Equal(t, []string{"ts", "reads", "errors", "p50_ms", "p95_ms", "p99_ms", "max_ms"}, rows[0])
	total := 0
	for _, r := range rows[1:] {
		n, _ := strconv.Atoi(r[1])
		total += n
	}
	require.Equal(t, int(s.Reads), total)
}

func TestReadLoadCountsErrors(t *testing.T) {
	view := newFakeView()
	view.getErr = errors.New("connection refused")
	rl := &ReadLoad{View: view, Rate: 100, Log: slog.New(slog.DiscardHandler)}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	rl.Run(ctx)
	s := rl.Summary()
	require.Positive(t, s.Reads)
	require.Equal(t, s.Reads, s.Errors)
	require.Zero(t, s.LatencyMs.P99, "las lecturas fallidas no entran en la latencia")
}

func TestProbeWithReadsOnlyAndRateZero(t *testing.T) {
	o := probeOptions()
	o.Rate, o.ReadRate, o.Duration = 0, 100, 200*time.Millisecond
	view := newFakeView()
	view.keys = []string{"pos:z-1:a"}
	sum, err := newProbeRunner(o, &projectingSender{view: view, targets: targetsFor(t, o)}, view, &bytes.Buffer{}).Run(context.Background())
	require.NoError(t, err)
	require.Zero(t, sum.Planned)
	require.NotNil(t, sum.Reads)
	require.Positive(t, sum.Reads.Reads)
	require.GreaterOrEqual(t, sum.ElapsedSec, 0.2, "la carga de lectura dura --duration")
}

// ---- opciones ----

func TestParseProbeOptions(t *testing.T) {
	base := map[string]string{"REGION": "r1", "AMQP_USER": "u", "AMQP_PASSWORD": "x", "REDIS_PASSWORD": "pw"}
	o, err := ParseOptions([]string{"--profile", "probe", "--prefix", "probe-e03-", "--meters", "50", "--read-rate", "200",
		"--probe-timeout", "90", "--poll", "20ms", "--read-out", "-"}, env(base), &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, 1.0, o.Rate, "1 sondeo/s por defecto")
	require.Equal(t, "market-view.voltera-r1:6379", o.RedisAddr)
	require.Equal(t, "pw", o.RedisPassword)
	require.Equal(t, 90*time.Second, o.ProbeTimeout)
	require.Equal(t, 20*time.Millisecond, o.ProbePoll)
	require.Equal(t, 200.0, o.ReadRate)

	o, err = ParseOptions([]string{"--profile", "probe", "--prefix", "probe-x-", "--rate", "0", "--read-rate", "10"}, env(base), &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, 0.0, o.Rate)

	cases := map[string][]string{
		"prefix":          {"--profile", "probe"},
		"no rate no read": {"--profile", "probe", "--prefix", "probe-a-", "--rate", "0"},
		"timeout":         {"--profile", "probe", "--prefix", "probe-a-", "--probe-timeout", "0"},
		"negative read":   {"--profile", "probe", "--prefix", "probe-a-", "--read-rate", "-1"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseOptions(args, env(base), &bytes.Buffer{})
			require.Error(t, err)
		})
	}
	noPW := map[string]string{"REGION": "r1", "AMQP_USER": "u", "AMQP_PASSWORD": "x"}
	_, err = ParseOptions([]string{"--profile", "probe", "--prefix", "probe-a-"}, env(noPW), &bytes.Buffer{})
	require.ErrorContains(t, err, "REDIS_PASSWORD")
	// otros perfiles no exigen Redis
	_, err = ParseOptions(nil, env(noPW), &bytes.Buffer{})
	require.NoError(t, err)
}

// ---- direct-positions ----

func TestDirectPositionsPublishesFullStateToEnergyPositions(t *testing.T) {
	o := testOptions(ProfileDirectPositions)
	o.Meters, o.Rate, o.Prefix, o.VersionBase = 20, 0, "e04-x-", 1_700_000_000
	fs := &fakeSender{}
	sum, err := newRunner(o, fs).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(20), sum.Published)
	require.Len(t, fs.sent, 20)

	ids := map[string]bool{}
	for _, m := range fs.sent {
		require.Equal(t, "energy-positions.x", m.Exchange)
		require.Equal(t, "energy-positions", m.RoutingKey)
		require.Equal(t, events.TypePositionDeclared, m.Type)
		env, err := events.Unmarshal(m.Body)
		require.NoError(t, err)
		require.Equal(t, m.MessageID, env.EventID)
		require.Equal(t, "r1", env.Region)
		require.Equal(t, int64(1_700_000_000), env.Version)
		require.False(t, env.CommittedAt.IsZero())
		require.False(t, env.EmittedAt.IsZero())
		require.Len(t, events.TraceID(env.Traceparent), 32)
		pos, err := events.DecodePayload[events.PositionDeclared](env)
		require.NoError(t, err)
		require.NoError(t, pos.Validate())
		require.Equal(t, env.AggregateID, pos.MeterID)
		require.Regexp(t, `^z-[1-4]$`, pos.Zone)
		require.True(t, pos.ValidUntil.After(env.OccurredAt))
		ids[env.EventID] = true
	}
	require.Len(t, ids, 20)
}
