package metersim

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func testOptions(profile Profile) Options {
	return Options{Profile: profile, Rate: 1000, Duration: time.Second, Meters: 200, Zones: 4, Seed: 7, DupK: 1,
		Prefix: "m-", Vendor: VendorMixed, Horizon: 120 * time.Second, Ramp: 5 * time.Second, Workers: 4, VersionBase: 1, Region: "r1"}
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

// ---- opciones ----

func TestParseOptionsDefaultsAndEnv(t *testing.T) {
	base := map[string]string{"REGION": "r1", "AMQP_USER": "meter-sim-r1", "AMQP_PASSWORD": "x"}
	o, err := ParseOptions(nil, env(base), &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, ProfileSteady, o.Profile)
	require.Equal(t, 100.0, o.Rate)
	require.Equal(t, 30*time.Second, o.Duration)
	require.Equal(t, 100, o.Meters)
	require.Equal(t, 4, o.Zones)
	require.Equal(t, "r1", o.AMQP.Vhost)
	require.Greater(t, o.VersionBase, int64(1_700_000_000), "steady usa el unix time: crece entre corridas")

	env2 := map[string]string{"METERSIM_PROFILE": "once", "METERSIM_METERS": "50", "METERSIM_RATE": "0", "METERSIM_PREFIX": "s16-"}
	for k, v := range base {
		env2[k] = v
	}
	o, err = ParseOptions([]string{"--meters", "200"}, env(env2), &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, ProfileOnce, o.Profile)
	require.Equal(t, 200, o.Meters, "el flag manda sobre la variable")
	require.Equal(t, 0.0, o.Rate, "once con --rate 0 = sin límite")
	require.Equal(t, int64(1), o.VersionBase)
	require.Equal(t, "s16-", o.Prefix)
}

func TestParseOptionsRateFromPeriodAndSecondsFlags(t *testing.T) {
	base := map[string]string{"REGION": "r2", "AMQP_USER": "u", "AMQP_PASSWORD": "x"}
	o, err := ParseOptions([]string{"--profile", "steady", "--meters", "300", "--period", "10", "--duration", "1m30s"}, env(base), &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, 30.0, o.Rate, "rate = meters / period")
	require.Equal(t, 90*time.Second, o.Duration)

	o, err = ParseOptions([]string{"--profile", "once", "--meters", "300", "--period", "10"}, env(base), &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, 100.0, o.Rate, "once ignora period")
}

func TestParseOptionsErrors(t *testing.T) {
	base := map[string]string{"REGION": "r1", "AMQP_USER": "u", "AMQP_PASSWORD": "x"}
	cases := map[string][]string{
		"profile":        {"--profile", "burst"},
		"rate zero":      {"--rate", "0"},
		"meters":         {"--meters", "0"},
		"pairs one":      {"--profile", "pairs", "--meters", "1"},
		"duration zero":  {"--duration", "0"},
		"bad duration":   {"--duration", "tomorrow"},
		"negative rate":  {"--rate", "-4"},
		"bad horizon":    {"--horizon", "0"},
		"empty vendor":   {"--vendor", ""},
		"unknown option": {"--nope"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseOptions(args, env(base), &bytes.Buffer{})
			require.Error(t, err)
		})
	}
	_, err := ParseOptions(nil, env(map[string]string{"METERSIM_METERS": "x"}), &bytes.Buffer{})
	require.ErrorContains(t, err, "METERS")
	require.ErrorContains(t, err, "REGION")
	require.ErrorContains(t, err, "AMQP_USER")
}

// ---- calendario ----

func TestCurveConstant(t *testing.T) {
	c := Curve{Rate: 100}
	require.Equal(t, 10*time.Millisecond, c.Time(1))
	require.Equal(t, time.Second, c.Time(100))
	require.Equal(t, 100, c.Count(time.Second))
	require.Equal(t, 3000, c.Count(30*time.Second))
	require.Equal(t, time.Duration(0), Curve{}.Time(5), "sin tasa = sin límite")
}

func TestCurveRampThenPlateau(t *testing.T) {
	c := Curve{Rate: 100, Ramp: 4 * time.Second}
	// la rampa de 4 s a 100/s acumula 200 declaraciones
	require.Equal(t, 200, c.Count(4*time.Second))
	require.InDelta(t, 4.0, c.Time(200).Seconds(), 1e-6)
	require.InDelta(t, 5.0, c.Time(300).Seconds(), 1e-6, "meseta a 100/s")
	require.Equal(t, 200+100*56, c.Count(60*time.Second))
	require.Equal(t, 50, c.Count(2*time.Second), "a mitad de la rampa, un cuarto del área")
	// Time y Count son consistentes
	for n := 1; n < 1000; n += 37 {
		require.GreaterOrEqual(t, c.Count(c.Time(n)+time.Millisecond), n, "n=%d", n)
	}
}

// ---- planes y formatos ----

func collect(p Plan) []Declaration {
	out := make([]Declaration, p.Total())
	for i := range out {
		out[i] = p.At(i)
	}
	return out
}

func TestOncePlanHasEveryMeterOnceWithBothVendors(t *testing.T) {
	p, _, err := NewPlan(testOptions(ProfileOnce))
	require.NoError(t, err)
	ds := collect(p)
	require.Len(t, ds, 200)
	vendors, ids, zones := map[string]int{}, map[string]bool{}, map[int]int{}
	for _, d := range ds {
		vendors[d.Vendor]++
		require.False(t, ids[d.MeterID])
		ids[d.MeterID] = true
		require.Equal(t, int64(1), d.Version)
		zones[d.Zone]++
	}
	require.Equal(t, map[string]int{acl.VendorAcme: 100, acl.VendorVoltix: 100}, vendors, "mitad y mitad")
	require.Equal(t, map[int]int{1: 50, 2: 50, 3: 50, 4: 50}, zones)
	require.Equal(t, "m-0001", ds[0].MeterID)
	require.Equal(t, "m-0200", ds[199].MeterID)
}

func TestVendorSplitIsBalancedInEveryZone(t *testing.T) {
	p, _, _ := NewPlan(testOptions(ProfileOnce))
	type key struct {
		zone   int
		vendor string
	}
	counts := map[key]int{}
	for _, d := range collect(p) {
		counts[key{d.Zone, d.Vendor}]++
	}
	for z := 1; z <= 4; z++ {
		require.Equal(t, 25, counts[key{z, acl.VendorAcme}], "zona %d", z)
		require.Equal(t, 25, counts[key{z, acl.VendorVoltix}], "zona %d", z)
	}
}

// Lo que meter-sim genera lo entiende la ACL del gateway, en ambos formatos, y coincide con la declaración.
func TestEncodedBodiesTranslateBackThroughTheACL(t *testing.T) {
	a, err := acl.New(4)
	require.NoError(t, err)
	p, _, _ := NewPlan(testOptions(ProfileSteady))
	ts := time.Date(2026, 10, 16, 10, 0, 0, 123456789, time.UTC)
	seen := map[string]int{}
	for i := range 400 {
		d := p.At(i)
		seen[d.Format]++
		body, err := d.Encode(ts)
		require.NoError(t, err)
		got, err := a.Translate(d.Vendor, body)
		require.NoError(t, err, string(body))
		require.Equal(t, d.MeterID, got.Position.MeterID)
		require.Equal(t, d.Side, got.Position.Side)
		require.InDelta(t, d.KWh, got.Position.KWh, 1e-9)
		require.Equal(t, d.Version, got.Version)
		require.Equal(t, "z-"+itoa(d.Zone), got.Position.Zone, "acme (derivada) y voltix (explícita) coinciden")
		require.True(t, got.OccurredAt.Equal(ts))
		require.True(t, got.Position.ValidUntil.Equal(ts.Add(120*time.Second)))
	}
	require.Equal(t, map[string]int{acl.VendorAcme: 200, acl.VendorVoltix: 200}, seen)
}

func itoa(n int) string { return string(rune('0' + n)) }

func TestUnknownVendorOptionSendsAcmeBodyWithOtherHeader(t *testing.T) {
	o := testOptions(ProfileOnce)
	o.Vendor = "mystery-v3"
	d := o.declare(1, 1)
	require.Equal(t, "mystery-v3", d.Vendor)
	body, err := d.Encode(time.Now())
	require.NoError(t, err)
	a, _ := acl.New(4)
	_, err = a.Translate(d.Vendor, body)
	var re *acl.RejectError
	require.ErrorAs(t, err, &re)
	require.Equal(t, acl.ReasonUnknownVendor, re.Reason)
	_, err = a.Translate(acl.VendorAcme, body)
	require.NoError(t, err, "el cuerpo en sí es válido")
}

func TestDupResendsTheOnceBatch(t *testing.T) {
	once, _, _ := NewPlan(testOptions(ProfileOnce))
	o := testOptions(ProfileDup)
	dup1, _, _ := NewPlan(o)
	require.Equal(t, sortedKeys(collect(once)), sortedKeys(collect(dup1)), "dup-k=1: mismo lote, mismos eventId y versiones")

	o.DupK = 3
	dup3, _, _ := NewPlan(o)
	require.Equal(t, 600, dup3.Total())
	perID := map[string]int{}
	for _, d := range collect(dup3) {
		perID[d.EventID]++
	}
	require.Len(t, perID, 200)
	for _, n := range perID {
		require.Equal(t, 3, n)
	}
	// mezclado y reproducible
	again, _, _ := NewPlan(o)
	require.Equal(t, collect(dup3), collect(again))
	require.NotEqual(t, collect(dup3)[:200], collect(once), "el orden no es el de once")
}

func sortedKeys(ds []Declaration) []string {
	m := make([]string, 0, len(ds))
	for _, d := range ds {
		m = append(m, d.EventID+"/"+d.MeterID+"/"+itoa(int(d.Version)))
	}
	sortStrings(m)
	return m
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func TestSeedChangesTheBatchAndEventIDsAreStableAcrossRuns(t *testing.T) {
	a, _, _ := NewPlan(testOptions(ProfileOnce))
	b, _, _ := NewPlan(testOptions(ProfileOnce))
	require.Equal(t, collect(a), collect(b))
	o := testOptions(ProfileOnce)
	o.Seed = 8
	c, _, _ := NewPlan(o)
	require.NotEqual(t, collect(a)[0].EventID, collect(c)[0].EventID)
	for _, d := range collect(a) {
		require.NoError(t, events.Envelope{EventID: d.EventID, Type: "x", SchemaVersion: 1, AggregateID: "a", OccurredAt: time.Now(), Payload: []byte("{}")}.Validate())
	}
}

func TestPairsHaveEqualKWhOppositeSidesAndOneZone(t *testing.T) {
	o := testOptions(ProfilePairs)
	o.Rate, o.Duration = 100, 4*time.Second // 400 declaraciones = 2 vueltas de 200
	p, _, err := NewPlan(o)
	require.NoError(t, err)
	require.Equal(t, 400, p.Total())
	require.Equal(t, 200, p.ExpectedPairs())
	ds := collect(p)
	for i := 0; i < len(ds); i += 2 {
		s, b := ds[i], ds[i+1]
		require.Equal(t, events.SideSurplus, s.Side)
		require.Equal(t, events.SideDeficit, b.Side)
		require.Equal(t, s.KWh, b.KWh, "igual kWh ⇒ un match por par")
		require.Equal(t, s.Zone, b.Zone)
		require.Equal(t, s.Version, b.Version)
		require.NotEqual(t, s.MeterID, b.MeterID)
	}
	// vuelta 2: versión +1 en todos
	require.Equal(t, ds[0].Version+1, ds[200].Version)
	require.Equal(t, ds[0].MeterID, ds[200].MeterID)

	zones := map[int]int{}
	for _, d := range ds[:200] {
		zones[d.Zone]++
	}
	require.Equal(t, map[int]int{1: 50, 2: 50, 3: 50, 4: 50}, zones)
}

func TestPairsOddTotalAndTooFewMeters(t *testing.T) {
	o := testOptions(ProfilePairs)
	o.Rate, o.Duration = 101, time.Second
	p, _, err := NewPlan(o)
	require.NoError(t, err)
	require.Equal(t, 100, p.Total(), "nunca queda un SURPLUS sin su DEFICIT")

	o.Meters, o.Zones = 3, 4 // ninguna zona llega a tener dos medidores
	_, _, err = NewPlan(o)
	require.Error(t, err)
}

func TestSteadyCyclesMetersAndRaisesVersions(t *testing.T) {
	o := testOptions(ProfileSteady)
	o.Meters, o.Rate, o.VersionBase = 10, 100, 1000
	p, _, _ := NewPlan(o)
	require.Equal(t, 100, p.Total())
	require.Equal(t, "m-0001", p.At(0).MeterID)
	require.Equal(t, int64(1000), p.At(0).Version)
	require.Equal(t, "m-0001", p.At(10).MeterID)
	require.Equal(t, int64(1001), p.At(10).Version)
	require.Equal(t, int64(1009), p.At(99).Version)
}

func TestSpikePlanRampsDown(t *testing.T) {
	o := testOptions(ProfileSpike)
	o.Rate, o.Duration, o.Ramp = 100, 10*time.Second, 4*time.Second
	p, c, err := NewPlan(o)
	require.NoError(t, err)
	require.Equal(t, 200+600, p.Total())
	require.Equal(t, 4*time.Second, c.Ramp)

	o.Duration = 2 * time.Second // la rampa se acota a la mitad de la duración
	_, c, _ = NewPlan(o)
	require.Equal(t, time.Second, c.Ramp)
}

// ---- corrida ----

type fakeSender struct {
	mu    sync.Mutex
	sent  []amqpx.Outgoing
	delay time.Duration
	fail  func(n int) error
}

func (f *fakeSender) Send(ctx context.Context, o amqpx.Outgoing) error {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		if err := f.fail(len(f.sent)); err != nil {
			return err
		}
	}
	f.sent = append(f.sent, o)
	return nil
}

func newRunner(o Options, s Sender) *Runner {
	svc := telemetry.New(telemetry.Info{Service: ServiceName, Region: o.Region}, slog.New(slog.DiscardHandler))
	return &Runner{Opts: o, Sender: s, Svc: svc, Log: slog.New(slog.DiscardHandler)}
}

func TestRunOnceUnthrottledPublishesEverythingWithHeadersAndCSV(t *testing.T) {
	o := testOptions(ProfileOnce)
	o.Rate = 0
	o.Out = filepath.Join(t.TempDir(), "sent.csv")
	fs := &fakeSender{}
	sum, err := newRunner(o, fs).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(200), sum.Published)
	require.Zero(t, sum.Errors)
	require.Zero(t, sum.Dropped)
	require.Equal(t, map[string]int{acl.VendorAcme: 100, acl.VendorVoltix: 100}, sum.ByVendor)

	tps := map[string]bool{}
	for _, m := range fs.sent {
		require.Equal(t, "meter-declarations.x", m.Exchange)
		require.Equal(t, "meter-declarations", m.RoutingKey)
		require.Contains(t, []any{acl.VendorAcme, acl.VendorVoltix}, m.Headers["x-vendor"])
		require.Equal(t, m.MessageID, m.Headers["eventId"])
		tp, _ := m.Headers["traceparent"].(string)
		require.Len(t, events.TraceID(tp), 32)
		require.False(t, tps[tp], "traceparent nuevo por declaración")
		tps[tp] = true
	}

	f, err := os.Open(o.Out)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Equal(t, []string{"eventId", "meterId", "version", "sentAt"}, rows[0])
	require.Len(t, rows, 201)
	_, err = time.Parse(time.RFC3339Nano, rows[1][3])
	require.NoError(t, err)
}

func TestRunSteadyKeepsTheArrivalRate(t *testing.T) {
	o := testOptions(ProfileSteady)
	o.Rate, o.Duration = 200, time.Second
	sum, err := newRunner(o, &fakeSender{}).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 200, sum.Planned)
	require.Equal(t, int64(200), sum.Published)
	require.InDelta(t, 1.0, sum.ElapsedSec, 0.15)
	require.InDelta(t, 200, sum.AchievedRate, 30)
	require.Less(t, sum.Late, int64(10))
	require.Zero(t, sum.Dropped)
}

func TestRunCountsDroppedAndLateWhenThePublisherCannotKeepUp(t *testing.T) {
	o := testOptions(ProfileSteady)
	o.Rate, o.Duration, o.Workers = 1000, 500*time.Millisecond, 1
	// un publicador de 1 mensaje cada 5 ms (200/s) ante 1000/s: la cola no se llena, pero casi todo sale tarde
	sum, err := newRunner(o, &fakeSender{delay: 5 * time.Millisecond}).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(500), sum.Scheduled)
	require.Greater(t, sum.Late, int64(300), "sale tarde, y se registra")
	require.Greater(t, sum.MaxLatenessMs, 100.0)
}

func TestRunDropsWhenTheQueueOverflows(t *testing.T) {
	o := testOptions(ProfileSteady)
	o.Rate, o.Duration, o.Workers = 5000, time.Second, 1
	r := newRunner(o, &fakeSender{delay: 5 * time.Millisecond})
	r.QueueDepth = 50
	sum, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Greater(t, sum.Dropped, int64(4000), "sin espacio en la cola se descarta, no se retrasa el calendario")
	require.Equal(t, int64(5000), sum.Scheduled+sum.Dropped)
}

func TestRunReportsPublishErrors(t *testing.T) {
	o := testOptions(ProfileOnce)
	o.Rate = 0
	calls := 0
	fs := &fakeSender{fail: func(int) error {
		calls++
		if calls == 11 {
			return errors.New("nack")
		}
		return nil
	}}
	sum, err := newRunner(o, fs).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), sum.Errors)
	require.Equal(t, int64(199), sum.Published)
}

func TestRunStopsOnCancel(t *testing.T) {
	o := testOptions(ProfileSteady)
	o.Rate, o.Duration = 10, time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	sum, err := newRunner(o, &fakeSender{}).Run(ctx)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 3*time.Second)
	require.Less(t, sum.Published, int64(10))
}
