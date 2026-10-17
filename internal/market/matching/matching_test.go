package matching

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

const validRules = `
params:
  minKwh: 0.5
  maxPrice: 0.40
eligible: "position.kwh >= params.minKwh && price.pricePerKwh <= params.maxPrice"
pair: "seller.zone == buyer.zone && seller.meterId != buyer.meterId"
`

var (
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
	t0    = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
)

func mustRules(t *testing.T, src string) *Rules {
	t.Helper()
	r, err := ParseRules([]byte(src))
	require.NoError(t, err)
	return r
}

func pos(id string, side events.Side, kwh float64, ver int64) Position {
	return Position{MeterID: id, Side: side, KWh: kwh, ValidUntil: t0.Add(time.Minute), Zone: "z-1", Version: ver}
}

func TestParseRulesRejectsBadInput(t *testing.T) {
	for name, src := range map[string]string{
		"not yaml":        "params: [",
		"unknown field":   "params: {}\neligible: 'true'\npair: 'true'\nextra: 1",
		"missing pair":    "eligible: 'true'",
		"syntax error":    "eligible: 'position.kwh >>'\npair: 'true'",
		"unknown var":     "eligible: 'nonexistent > 1'\npair: 'true'",
		"unknown field 2": "eligible: 'position.nope > 1'\npair: 'true'",
		"not bool":        "eligible: 'position.kwh'\npair: 'true'",
	} {
		_, err := ParseRules([]byte(src))
		require.Error(t, err, name)
	}
}

func TestRulesEvaluate(t *testing.T) {
	r := mustRules(t, validRules)
	price := Price{PricePerKWh: 0.30}
	ok, err := r.Eligible(pos("a", events.SideSurplus, 1, 1), price)
	require.NoError(t, err)
	require.True(t, ok)
	ok, _ = r.Eligible(pos("a", events.SideSurplus, 0.1, 1), price)
	require.False(t, ok, "below minKwh")
	ok, _ = r.Eligible(pos("a", events.SideSurplus, 1, 1), Price{PricePerKWh: 0.5})
	require.False(t, ok, "price above maxPrice")

	ok, err = r.CanPair(pos("a", events.SideSurplus, 1, 1), pos("b", events.SideDeficit, 1, 1), price)
	require.NoError(t, err)
	require.True(t, ok)
	other := pos("b", events.SideDeficit, 1, 1)
	other.Zone = "z-2"
	ok, _ = r.CanPair(pos("a", events.SideSurplus, 1, 1), other, price)
	require.False(t, ok)
}

func TestPlanZoneDeterministicOrderAndMin(t *testing.T) {
	r := mustRules(t, validRules)
	price := Price{PricePerKWh: 0.3}
	in := []Position{
		pos("s-b", events.SideSurplus, 5, 1),
		pos("s-a", events.SideSurplus, 5, 1), // empate de kwh: meterId asc -> s-a antes que s-b
		pos("s-c", events.SideSurplus, 9, 1),
		pos("d-1", events.SideDeficit, 3, 1),
		pos("d-2", events.SideDeficit, 7, 1),
		pos("tiny", events.SideDeficit, 0.1, 1),
	}
	plan := PlanZone(in, price, r, t0, quiet)
	require.Len(t, plan.Pairs, 2)
	// s-c(9) con d-2(7): kwh=min=7; s-a(5) con d-1(3): kwh=3; s-b queda sin pareja
	require.Equal(t, "s-c", plan.Pairs[0].Seller.MeterID)
	require.Equal(t, "d-2", plan.Pairs[0].Buyer.MeterID)
	require.Equal(t, 7.0, plan.Pairs[0].KWh)
	require.Equal(t, "s-a", plan.Pairs[1].Seller.MeterID)
	require.Equal(t, "d-1", plan.Pairs[1].Buyer.MeterID)
	require.Equal(t, 3.0, plan.Pairs[1].KWh)
	require.Equal(t, 1, plan.SurplusLeft)
	require.Equal(t, 5, plan.Eligible) // "tiny" no es elegible

	// mismo resultado con otro orden de entrada
	rev := make([]Position, len(in))
	for i, p := range in {
		rev[len(in)-1-i] = p
	}
	require.Equal(t, plan.Pairs, PlanZone(rev, price, r, t0, quiet).Pairs)
}

func TestPlanZoneSkipsExpiredAndRuleErrors(t *testing.T) {
	r := mustRules(t, validRules)
	expired := pos("old", events.SideSurplus, 5, 1)
	expired.ValidUntil = t0.Add(-time.Second)
	plan := PlanZone([]Position{expired, pos("d", events.SideDeficit, 5, 1)}, Price{PricePerKWh: 0.3}, r, t0, quiet)
	require.Empty(t, plan.Pairs)
	require.Equal(t, 1, plan.Considered)

	// regla que falla en ejecución (división de un valor desconocido): el par se trata como "no"
	bad := mustRules(t, "params: {}\neligible: 'true'\npair: 'params.missing.deep > 1'")
	plan = PlanZone([]Position{pos("s", events.SideSurplus, 5, 1), pos("d", events.SideDeficit, 5, 1)}, Price{}, bad, t0, quiet)
	require.Empty(t, plan.Pairs)
	require.Positive(t, plan.RuleErrors)
}

func TestRulesLoaderAtomicReloadKeepsPreviousOnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, RulesFileName)
	require.NoError(t, os.WriteFile(path, []byte(validRules), 0o600))
	reg := prometheus.NewRegistry()
	l, err := NewRulesLoader(dir, quiet, reg)
	require.NoError(t, err)
	v1 := l.Current().Version

	changed, err := l.Reload()
	require.NoError(t, err)
	require.False(t, changed, "same content: nothing to do")

	// regla nueva válida: se publica
	require.NoError(t, os.WriteFile(path, []byte(validRules+"\n# edit\nparams2: 1\n"), 0o600))
	_, err = l.Reload()
	require.Error(t, err, "params2 is not a known field")
	require.Equal(t, v1, l.Current().Version, "previous rules kept")
	require.NotEmpty(t, l.State().LastReloadError)

	require.NoError(t, os.WriteFile(path, []byte(`
params: {minKwh: 2}
eligible: "position.kwh >= params.minKwh"
pair: "true"
`), 0o600))
	changed, err = l.Reload()
	require.NoError(t, err)
	require.True(t, changed)
	require.NotEqual(t, v1, l.Current().Version)
	require.Empty(t, l.State().LastReloadError)
	require.Equal(t, 2, l.Current().Params["minKwh"])
	require.Equal(t, 2.0, promtest.ToFloat64(l.reloads.WithLabelValues("ok")))
	require.Equal(t, 1.0, promtest.ToFloat64(l.reloads.WithLabelValues("error")))
}

func TestRulesLoaderFailsOnInvalidInitialRules(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, RulesFileName), []byte("eligible: 'x'"), 0o600))
	_, err := NewRulesLoader(dir, quiet, nil)
	require.Error(t, err)
}

func TestRulesLoaderRunPicksUpChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, RulesFileName)
	require.NoError(t, os.WriteFile(path, []byte(validRules), 0o600))
	l, err := NewRulesLoader(dir, quiet, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx, 20*time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte("params: {}\neligible: 'true'\npair: 'false'\n"), 0o600))
	require.Eventually(t, func() bool { return l.Current().PairSrc == "false" }, 3*time.Second, 20*time.Millisecond)
}

// --- integración con Postgres (TEST_POSTGRES_DSN) ---

func newStore(t *testing.T) (*Store, func(sql string, args ...any) int) {
	t.Helper()
	pool := testutil.Postgres(t)
	require.NoError(t, migrate.Up(context.Background(), pool, outbox.Migrations(), Migrations()))
	count := func(sql string, args ...any) int {
		var n int
		require.NoError(t, pool.QueryRow(context.Background(), sql, args...).Scan(&n))
		return n
	}
	return NewStore(pool, "r1"), count
}

func TestStorePersistsMatchOutboxAndPositionsAtomically(t *testing.T) {
	st, count := newStore(t)
	ctx := context.Background()
	pair := Pair{Seller: pos("s1", events.SideSurplus, 5, 3), Buyer: pos("d1", events.SideDeficit, 4, 7), KWh: 4}

	created, discarded, err := st.Persist(ctx, "w1", "z-1", Price{PricePerKWh: 0.2}, []Pair{pair})
	require.NoError(t, err)
	require.Zero(t, discarded)
	require.Len(t, created, 1)
	c := created[0]
	require.Equal(t, events.MatchID("w1", "s1@3", "d1@7"), c.Payload.MatchID)
	require.Equal(t, events.TypeEnergyMatched, c.Event.Type)
	require.NotEmpty(t, c.Event.TraceID())

	require.Equal(t, 1, count(`SELECT count(*) FROM matches WHERE match_id = $1 AND kwh = 4 AND price_per_kwh = 0.2 AND zone = 'z-1' AND "window" = 'w1'`, c.Payload.MatchID))
	require.Equal(t, 2, count(`SELECT count(*) FROM matched_positions WHERE match_id = $1`, c.Payload.MatchID))
	require.Equal(t, 1, count(`SELECT count(*) FROM outbox WHERE topic = 'r1.energy-matched' AND key = $1 AND published_at IS NULL`, c.Payload.MatchID))

	// la misma versión de la posición no se empareja otra vez, ni en otra ventana
	again := Pair{Seller: pair.Seller, Buyer: pos("d2", events.SideDeficit, 4, 1), KWh: 4}
	created, discarded, err = st.Persist(ctx, "w2", "z-1", Price{PricePerKWh: 0.2}, []Pair{again})
	require.NoError(t, err)
	require.Empty(t, created)
	require.Equal(t, 1, discarded)
	require.Equal(t, 1, count(`SELECT count(*) FROM matches`))
	require.Equal(t, 2, count(`SELECT count(*) FROM matched_positions`), "the rolled-back pair left no marks")
	require.Equal(t, 1, count(`SELECT count(*) FROM outbox`))

	// una versión nueva de la misma posición sí puede emparejarse
	newer := Pair{Seller: pos("s1", events.SideSurplus, 5, 4), Buyer: pos("d2", events.SideDeficit, 4, 1), KWh: 4}
	created, _, err = st.Persist(ctx, "w2", "z-1", Price{PricePerKWh: 0.2}, []Pair{newer})
	require.NoError(t, err)
	require.Len(t, created, 1)
}

func TestStoreSameWindowSamePairIsIdempotent(t *testing.T) {
	st, count := newStore(t)
	pair := Pair{Seller: pos("s1", events.SideSurplus, 5, 1), Buyer: pos("d1", events.SideDeficit, 5, 1), KWh: 5}
	for i := 0; i < 2; i++ {
		_, _, err := st.Persist(context.Background(), "w1", "z-1", Price{PricePerKWh: 0.2}, []Pair{pair})
		require.NoError(t, err)
	}
	require.Equal(t, 1, count(`SELECT count(*) FROM matches`))
	require.Equal(t, 1, count(`SELECT count(*) FROM outbox`))
}

type fakeSource struct {
	positions map[string][]Position
	price     Price
}

func (f fakeSource) Positions(context.Context) (map[string][]Position, int, error) {
	return f.positions, 0, nil
}
func (f fakeSource) Price(context.Context, string) (Price, error) { return f.price, nil }

func TestEngineRunWindowCreatesMatchesPerZone(t *testing.T) {
	st, count := newStore(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, RulesFileName), []byte(validRules), 0o600))
	rl, err := NewRulesLoader(dir, quiet, nil)
	require.NoError(t, err)

	src := fakeSource{price: Price{PricePerKWh: 0.3, ValidUntil: t0.Add(time.Hour)}, positions: map[string][]Position{}}
	for z := 1; z <= 2; z++ {
		zone := fmt.Sprintf("z-%d", z)
		for i := 0; i < 3; i++ {
			s := pos(fmt.Sprintf("%s-s%d", zone, i), events.SideSurplus, float64(5+i), 1)
			s.Zone = zone
			d := pos(fmt.Sprintf("%s-d%d", zone, i), events.SideDeficit, float64(4+i), 1)
			d.Zone = zone
			src.positions[zone] = append(src.positions[zone], s, d)
		}
	}
	woken := 0
	reg := prometheus.NewRegistry()
	m := telemetry.NewDomainMetrics(reg)
	eng := &Engine{Region: "r1", Window: 2 * time.Second, Source: src, Store: st, Rules: rl, Metrics: m, Log: quiet,
		OnCommit: func() { woken++ }, Now: func() time.Time { return t0 }}

	require.NoError(t, eng.RunWindow(context.Background()))
	require.Equal(t, 6, count(`SELECT count(*) FROM matches`))
	require.Equal(t, 6, count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`))
	require.Equal(t, 2, woken)
	require.Equal(t, 6.0, promtest.ToFloat64(m.MatchesCreated.WithLabelValues("r1")))
	require.Equal(t, 6, eng.LastWindow().Created)
	require.Equal(t, "2026-10-16T12:00:00Z", eng.LastWindow().Window)

	// misma vista en la ventana siguiente: las posiciones ya emparejadas ni se proponen
	eng.Now = func() time.Time { return t0.Add(2 * time.Second) }
	require.NoError(t, eng.RunWindow(context.Background()))
	require.Equal(t, 6, count(`SELECT count(*) FROM matches`))
	require.Zero(t, eng.LastWindow().Created)
	require.Zero(t, eng.LastWindow().Discarded)
	require.Zero(t, eng.LastWindow().Eligible)

	// una versión nueva de una posición ya emparejada sí entra otra vez
	again := src.positions["z-1"][0]
	again.Version++
	src.positions["z-1"][0] = again
	eng.Now = func() time.Time { return t0.Add(4 * time.Second) }
	require.NoError(t, eng.RunWindow(context.Background()))
	require.Equal(t, 1, eng.LastWindow().Eligible)
}

func TestEngineSkipsWindowWithoutValidPrice(t *testing.T) {
	st, count := newStore(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, RulesFileName), []byte(validRules), 0o600))
	rl, err := NewRulesLoader(dir, quiet, nil)
	require.NoError(t, err)
	src := fakeSource{price: Price{PricePerKWh: 0.3, ValidUntil: t0.Add(-time.Second)},
		positions: map[string][]Position{"z-1": {pos("s", events.SideSurplus, 5, 1), pos("d", events.SideDeficit, 5, 1)}}}
	eng := &Engine{Region: "r1", Window: 2 * time.Second, Source: src, Store: st, Rules: rl,
		Metrics: telemetry.NewDomainMetrics(prometheus.NewRegistry()), Log: quiet, Now: func() time.Time { return t0 }}
	require.NoError(t, eng.RunWindow(context.Background()))
	require.Zero(t, count(`SELECT count(*) FROM matches`))
	require.True(t, eng.LastWindow().NoPrice)
}

// --- control de E6: OUTBOX_MODE=direct (escritura dual ingenua) ---

func TestParseOutboxMode(t *testing.T) {
	for in, want := range map[string]OutboxMode{"": ModeOutbox, "outbox": ModeOutbox, "direct": ModeDirect} {
		got, err := ParseOutboxMode(in)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := ParseOutboxMode("off")
	require.Error(t, err)
}

func TestLoadConfigOutboxMode(t *testing.T) {
	env := map[string]string{"REGION": "r1", "DATABASE_URL": "postgres://x", "REDIS_ADDR": "redis:6379"}
	get := func(k string) string { return env[k] }
	c, err := LoadConfig(get)
	require.NoError(t, err)
	require.Equal(t, ModeOutbox, c.OutboxMode, "default")
	env["OUTBOX_MODE"] = "direct"
	c, err = LoadConfig(get)
	require.NoError(t, err)
	require.Equal(t, ModeDirect, c.OutboxMode)
	env["OUTBOX_MODE"] = "nope"
	_, err = LoadConfig(get)
	require.ErrorContains(t, err, "OUTBOX_MODE")
}

type recPublisher struct{ msgs []outbox.Message }

func (r *recPublisher) Publish(_ context.Context, m outbox.Message) error {
	r.msgs = append(r.msgs, m)
	return nil
}

func directEngine(t *testing.T, st *Store, pub outbox.Publisher, fp *failpoint.Registry, n int) *Engine {
	t.Helper()
	st.Mode = ModeDirect
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, RulesFileName), []byte(validRules), 0o600))
	rl, err := NewRulesLoader(dir, quiet, nil)
	require.NoError(t, err)
	src := fakeSource{price: Price{PricePerKWh: 0.3, ValidUntil: t0.Add(time.Hour)}, positions: map[string][]Position{}}
	for i := 0; i < n; i++ {
		src.positions["z-1"] = append(src.positions["z-1"],
			pos(fmt.Sprintf("s%d", i), events.SideSurplus, 5, 1), pos(fmt.Sprintf("d%d", i), events.SideDeficit, 5, 1))
	}
	return &Engine{Region: "r1", Window: 2 * time.Second, Source: src, Store: st, Rules: rl,
		Metrics: telemetry.NewDomainMetrics(prometheus.NewRegistry()), Log: quiet, Now: func() time.Time { return t0 },
		Direct: pub, Failpoints: fp}
}

func TestDirectModeCommitsWithoutOutboxAndPublishesDirectly(t *testing.T) {
	st, count := newStore(t)
	pub := &recPublisher{}
	eng := directEngine(t, st, pub, nil, 3)
	require.NoError(t, eng.RunWindow(context.Background()))
	require.Equal(t, 3, count(`SELECT count(*) FROM matches`))
	require.Zero(t, count(`SELECT count(*) FROM outbox`), "direct mode leaves no outbox row")
	require.Len(t, pub.msgs, 3)
	require.Equal(t, "r1.energy-matched", pub.msgs[0].Topic)
	require.Equal(t, events.TypeEnergyMatched, pub.msgs[0].Type)
}

func TestDirectModeCrashBetweenCommitAndPublishLosesTheEvent(t *testing.T) {
	st, count := newStore(t)
	pub := &recPublisher{}
	fp := failpoint.New(failpoint.WithExit(func(int) { panic("crash") }))
	require.NoError(t, fp.Set(outbox.FPAfterCommitBeforePublish, "crash-after:1"))
	eng := directEngine(t, st, pub, fp, 3)

	require.PanicsWithValue(t, "crash", func() { _ = eng.RunWindow(context.Background()) })
	require.Equal(t, 3, count(`SELECT count(*) FROM matches`), "the matches were committed")
	require.Len(t, pub.msgs, 1, "only the first one made it out")

	// tras el reinicio nada reenvía los otros dos: están emparejados y no hay outbox
	eng2 := directEngine(t, st, pub, nil, 3)
	require.NoError(t, eng2.RunWindow(context.Background()))
	require.Len(t, pub.msgs, 1, "lost matches are never republished")
	require.Equal(t, 3, count(`SELECT count(*) FROM matches`))
}

func TestOutboxModeStoresEventWithTheMatch(t *testing.T) {
	st, count := newStore(t)
	eng := directEngine(t, st, nil, nil, 3)
	st.Mode = ModeOutbox
	eng.Direct = nil
	require.NoError(t, eng.RunWindow(context.Background()))
	// match y evento se confirman juntos: aunque el proceso muera, el relay encontrará la fila
	require.Equal(t, 3, count(`SELECT count(*) FROM matches`))
	require.Equal(t, 3, count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`))
}
