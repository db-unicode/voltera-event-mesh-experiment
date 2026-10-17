package projection

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

type fakeView struct {
	versions map[string]int64
	fields   map[string]map[string]any
	ttl      map[string]time.Duration
	err      error
}

func newFakeView() *fakeView {
	return &fakeView{versions: map[string]int64{}, fields: map[string]map[string]any{}, ttl: map[string]time.Duration{}}
}

func (f *fakeView) Upsert(_ context.Context, key string, version int64, fields map[string]any, ttl time.Duration) (view.Result, error) {
	if f.err != nil {
		return "", f.err
	}
	if f.versions[key] >= version {
		return view.Stale, nil
	}
	f.versions[key], f.fields[key], f.ttl[key] = version, fields, ttl
	return view.Applied, nil
}

type rig struct {
	p    *Projector
	view *fakeView
	svc  *telemetry.Service
	fp   *failpoint.Registry
	now  time.Time
}

func newRig(t *testing.T) *rig {
	t.Helper()
	svc := telemetry.New(telemetry.Info{Service: ServiceName, Region: "r1"}, slog.New(slog.DiscardHandler))
	r := &rig{view: newFakeView(), svc: svc, fp: failpoint.New(), now: time.Date(2026, 10, 16, 10, 0, 1, 500_000_000, time.UTC)}
	r.p = &Projector{Region: "r1", View: r.view, Svc: svc, PositionTTL: 150 * time.Second, PriceTTL: 300 * time.Second,
		Failpoints: r.fp, Dropped: NewDroppedCounter(svc.Reg), Clock: func() time.Time { return r.now }}
	return r
}

func positionBody(t *testing.T, meter, zone string, version int64, mutate func(*events.Envelope)) []byte {
	t.Helper()
	occurred := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	env, err := events.NewPositionDeclared("r1", version, occurred, events.PositionDeclared{
		MeterID: meter, Side: events.SideDeficit, KWh: 3.1, ValidUntil: occurred.Add(2 * time.Minute), Zone: zone})
	require.NoError(t, err)
	env.CommittedAt = occurred.Add(200 * time.Millisecond)
	env.EmittedAt = occurred.Add(300 * time.Millisecond)
	if mutate != nil {
		mutate(&env)
	}
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func deliver(body []byte) amqpx.Delivery { return amqpx.Delivery{Body: body} }

func TestPositionIsProjectedPerContract(t *testing.T) {
	r := newRig(t)
	body := positionBody(t, "m-0002", "z-3", 9, nil)
	require.NoError(t, r.p.HandlePosition(context.Background(), deliver(body)))

	f := r.view.fields["pos:z-3:m-0002"]
	require.NotNil(t, f)
	env, err := events.Unmarshal(body)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"meterId": "m-0002", "side": "DEFICIT", "kwh": "3.1", "validUntil": "2026-10-16T10:02:00Z", "zone": "z-3", "region": "r1",
		"eventId": env.EventID, "occurredAt": "2026-10-16T10:00:00Z", "committedAt": "2026-10-16T10:00:00.2Z",
		"projectedAt": "2026-10-16T10:00:01.5Z",
	}, f)
	require.Equal(t, int64(9), r.view.versions["pos:z-3:m-0002"], "versión de la vista = Envelope.Version")
	require.Equal(t, 150*time.Second, r.view.ttl["pos:z-3:m-0002"])
	require.Equal(t, 1.0, promtest.ToFloat64(r.svc.Metrics.EventsConsumed.WithLabelValues("PositionDeclared", QueuePositions, "ok")))
}

func TestPositionLagIsObservedPerStage(t *testing.T) {
	r := newRig(t)
	require.NoError(t, r.p.HandlePosition(context.Background(), deliver(positionBody(t, "m-1", "z-1", 1, nil))))
	// aplicado a 10:00:01.5; occurred 10:00:00, committed 10:00:00.2, emitted 10:00:00.3
	for stage, want := range map[string]float64{"occurred": 1.5, "committed": 1.3, "emitted": 1.2} {
		var m dto.Metric
		obs := r.svc.Metrics.E2ELagSeconds.WithLabelValues("PositionDeclared", stage)
		require.NoError(t, obs.(prometheus.Metric).Write(&m))
		require.Equal(t, uint64(1), m.GetHistogram().GetSampleCount(), stage)
		require.InDelta(t, want, m.GetHistogram().GetSampleSum(), 1e-6, stage)
	}
}

func TestStaleAndDuplicateAreAckedAndCounted(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	require.NoError(t, r.p.HandlePosition(ctx, deliver(positionBody(t, "m-1", "z-1", 5, nil))))
	require.NoError(t, r.p.HandlePosition(ctx, deliver(positionBody(t, "m-1", "z-1", 5, nil))))
	require.NoError(t, r.p.HandlePosition(ctx, deliver(positionBody(t, "m-1", "z-1", 4, nil))))
	require.Equal(t, 2.0, promtest.ToFloat64(r.svc.Metrics.StaleDropped.WithLabelValues("PositionDeclared")))
	require.Equal(t, int64(5), r.view.versions["pos:z-1:m-1"])
}

func TestDropPositionFailpointAcksAndDiscards(t *testing.T) {
	r := newRig(t)
	require.NoError(t, r.fp.Set(FPDropPosition, "drop"))
	require.NoError(t, r.p.HandlePosition(context.Background(), deliver(positionBody(t, "m-1", "z-1", 1, nil))))
	require.Empty(t, r.view.versions, "no llega a la vista")
	require.Equal(t, 1.0, promtest.ToFloat64(r.p.Dropped.WithLabelValues("PositionDeclared")))

	require.NoError(t, r.fp.Set(FPDropPosition, "off"))
	require.NoError(t, r.p.HandlePosition(context.Background(), deliver(positionBody(t, "m-1", "z-1", 2, nil))))
	require.Len(t, r.view.versions, 1)
}

func TestInvalidPositionsAreRejected(t *testing.T) {
	r := newRig(t)
	cases := map[string][]byte{
		"not json":      []byte("nope"),
		"wrong type":    positionBody(t, "m-1", "z-1", 1, func(e *events.Envelope) { e.Type = events.TypePriceUpdated }),
		"other region":  positionBody(t, "m-1", "z-1", 1, func(e *events.Envelope) { e.Region = "r2" }),
		"bad zone":      positionBody(t, "m-1", "zone-1", 1, nil),
		"empty zone":    positionBody(t, "m-1", "", 1, nil),
		"aggregate":     positionBody(t, "m-1", "z-1", 1, func(e *events.Envelope) { e.AggregateID = "m-2" }),
		"version zero":  positionBody(t, "m-1", "z-1", 0, nil),
		"empty payload": positionBody(t, "m-1", "z-1", 1, func(e *events.Envelope) { e.Payload = json.RawMessage(`{"side":"NOPE"}`) }),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			err := r.p.HandlePosition(context.Background(), deliver(body))
			var ie *amqpx.InvalidError
			require.ErrorAs(t, err, &ie)
			require.Equal(t, "PositionDeclared", ie.EventType)
		})
	}
	require.Empty(t, r.view.versions)
}

func TestRedisErrorIsTransient(t *testing.T) {
	r := newRig(t)
	r.view.err = context.DeadlineExceeded
	err := r.p.HandlePosition(context.Background(), deliver(positionBody(t, "m-1", "z-1", 1, nil)))
	require.Error(t, err)
	var ie *amqpx.InvalidError
	require.NotErrorAs(t, err, &ie)
}

func priceBody(t *testing.T, region string, price float64, version int64) []byte {
	t.Helper()
	now := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	env, err := events.NewPriceUpdated(region, version, now, events.PriceUpdated{Region: region, PricePerKWh: price, ValidUntil: now.Add(30 * time.Second)})
	require.NoError(t, err)
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func TestPriceIsProjectedPerContract(t *testing.T) {
	r := newRig(t)
	require.NoError(t, r.p.HandlePrice(context.Background(), deliver(priceBody(t, "r1", 0.1525, 1760608800000))))
	f := r.view.fields["price:r1"]
	require.Equal(t, "0.1525", f["pricePerKwh"])
	require.Equal(t, "2026-10-16T10:00:30Z", f["validUntil"])
	require.Equal(t, "2026-10-16T10:00:00Z", f["occurredAt"])
	require.NotEmpty(t, f["eventId"])
	require.Equal(t, 300*time.Second, r.view.ttl["price:r1"])

	require.NoError(t, r.p.HandlePrice(context.Background(), deliver(priceBody(t, "r1", 0.2, 1760608800000)))) // misma versión: stale
	require.Equal(t, "0.1525", r.view.fields["price:r1"]["pricePerKwh"])
}

func TestInvalidPricesAreRejected(t *testing.T) {
	r := newRig(t)
	for name, body := range map[string][]byte{
		"other region": priceBody(t, "r2", 0.1, 1),
		"zero price":   priceBody(t, "r1", 0, 1),
		"garbage":      []byte("{"),
	} {
		t.Run(name, func(t *testing.T) {
			var ie *amqpx.InvalidError
			require.ErrorAs(t, r.p.HandlePrice(context.Background(), deliver(body)), &ie)
			require.Equal(t, "PriceUpdated", ie.EventType)
		})
	}
}

func TestKeys(t *testing.T) {
	require.Equal(t, "pos:z-3:m-0002", PositionKey("z-3", "m-0002"))
	require.Equal(t, "price:r2", PriceKey("r2"))
}

func TestConfigFromEnv(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	c, err := ConfigFromEnv(get(map[string]string{"REGION": "r2", "AMQP_USER": "u", "AMQP_PASSWORD": "p", "REDIS_PASSWORD": "x"}))
	require.NoError(t, err)
	require.Equal(t, "market-view.voltera-r2:6379", c.RedisAddr)
	require.Equal(t, 150*time.Second, c.PositionTTL)
	require.Equal(t, 300*time.Second, c.PriceTTL)
	require.Equal(t, "r2", c.AMQP.Vhost)

	_, err = ConfigFromEnv(get(map[string]string{"VIEW_TTL_S": "0"}))
	require.ErrorContains(t, err, "REGION")
	require.ErrorContains(t, err, "REDIS_PASSWORD")
	require.ErrorContains(t, err, "VIEW_TTL_S")
	require.ErrorContains(t, err, "AMQP_USER")
}

// Con Redis real (TEST_REDIS_ADDR): el proyector escribe con view.Store y el TTL del contrato.
func TestProjectsIntoRealRedis(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	svc := telemetry.New(telemetry.Info{Service: ServiceName, Region: "r1"}, slog.New(slog.DiscardHandler))
	p := &Projector{Region: "r1", View: view.New(rdb, svc.Reg), Svc: svc, PositionTTL: 150 * time.Second, PriceTTL: 300 * time.Second,
		Dropped: NewDroppedCounter(svc.Reg)}
	// El prefijo de test aísla las keys: el meterId lo lleva (la key sigue siendo pos:<zona>:<meterId>).
	meter := "t" + prefix[2:len(prefix)-1]
	body := positionBody(t, meter, "z-2", 3, nil)
	require.NoError(t, p.HandlePosition(context.Background(), deliver(body)))
	t.Cleanup(func() { rdb.Del(context.Background(), PositionKey("z-2", meter)) })

	e, err := view.New(rdb, nil).Get(context.Background(), PositionKey("z-2", meter))
	require.NoError(t, err)
	require.Equal(t, int64(3), e.Version)
	require.Equal(t, "z-2", e.Fields["zone"])
	require.Equal(t, "DEFICIT", e.Fields["side"])
	require.InDelta(t, 150, e.TTL.Seconds(), 2)
}
