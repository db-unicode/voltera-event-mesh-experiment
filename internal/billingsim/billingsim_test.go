package billingsim

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/natsx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newBiller() *Biller {
	return &Biller{Ledger: NewLedger(), Log: quiet,
		Svc:     telemetry.New(telemetry.Info{Service: "billing-sim", Region: "global"}, quiet),
		Metrics: telemetry.NewDomainMetrics(prometheus.NewRegistry())}
}

func delivery(t *testing.T, region, matchID string) natsx.Delivery {
	t.Helper()
	env, err := events.NewEnergyMatched(region, 1, time.Now(), events.EnergyMatched{MatchID: matchID, SellerMeterID: "s", BuyerMeterID: "b", KWh: 1, Zone: "z", Window: "w"})
	require.NoError(t, err)
	body, err := events.Marshal(env)
	require.NoError(t, err)
	return natsx.Delivery{Subject: "energy.matched." + region + "." + matchID, Data: body, Header: nats.Header{}}
}

func TestBillingIsIdempotentPerMatchIDAndSplitsRegions(t *testing.T) {
	b := newBiller()
	ctx := context.Background()
	require.NoError(t, b.Handle(ctx, delivery(t, "r1", "m1")))
	require.NoError(t, b.Handle(ctx, delivery(t, "r1", "m2")))
	require.NoError(t, b.Handle(ctx, delivery(t, "r2", "m3")))
	require.NoError(t, b.Handle(ctx, delivery(t, "r1", "m1"))) // repetido

	st := b.Ledger.Snapshot(true)
	require.Equal(t, 4, st.Total)
	require.Equal(t, 3, st.Distinct)
	require.Equal(t, 1, st.Duplicates)
	require.Equal(t, RegionStats{Total: 3, Distinct: 2, Duplicates: 1}, st.ByRegion["r1"])
	require.Equal(t, RegionStats{Total: 1, Distinct: 1}, st.ByRegion["r2"])
	require.Equal(t, 2, st.ByMatchID["m1"].Count)
	require.Equal(t, "r1", st.ByMatchID["m1"].Region)
	require.Equal(t, 1, st.ByMatchID["m2"].Count)
	require.Equal(t, "r2", st.ByMatchID["m3"].Region)
	require.Nil(t, b.Ledger.Snapshot(false).ByMatchID)

	require.Equal(t, 3.0, promtest.ToFloat64(b.Metrics.BillingReceived.WithLabelValues("r1")))
	require.Equal(t, 1.0, promtest.ToFloat64(b.Metrics.BillingDuplicates.WithLabelValues("r1")))
	require.Zero(t, promtest.ToFloat64(b.Metrics.BillingDuplicates.WithLabelValues("r2")))
}

func TestPoisonMessageIsAckedAndIgnored(t *testing.T) {
	b := newBiller()
	require.NoError(t, b.Handle(context.Background(), natsx.Delivery{Subject: "energy.matched.r1.x", Data: []byte("nope"), Header: nats.Header{}}))
	require.Zero(t, b.Ledger.Snapshot(false).Total)
}

func TestStateEndpoint(t *testing.T) {
	b := newBiller()
	require.NoError(t, b.Handle(context.Background(), delivery(t, "r1", "m1")))
	ts := httptest.NewServer(Handler(b.Ledger))
	defer ts.Close()

	get := func(path string) State {
		resp, err := http.Get(ts.URL + path)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		var st State
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
		return st
	}
	st := get("/state")
	require.Equal(t, 1, st.Distinct)
	require.Nil(t, st.ByMatchID)
	e := get("/state?detail=1").ByMatchID["m1"]
	require.Equal(t, "r1", e.Region)
	require.Equal(t, 1, e.Count)
}

func TestLedgerKeepsFirstDeliveryTimesAndLatencies(t *testing.T) {
	l := NewLedger()
	occurred := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	emitted := occurred.Add(100 * time.Millisecond)
	first := occurred.Add(350 * time.Millisecond)

	require.False(t, l.RecordAt("r2", "m1", Times{ReceivedAt: first, EmittedAt: emitted, OccurredAt: occurred}))
	// el duplicado llega mucho después y no mueve los sellos de la primera entrega
	require.True(t, l.RecordAt("r2", "m1", Times{ReceivedAt: first.Add(time.Minute), EmittedAt: emitted, OccurredAt: occurred}))

	e := l.Snapshot(true).ByMatchID["m1"]
	require.Equal(t, 2, e.Count)
	require.True(t, first.Equal(e.ReceivedAt))
	require.InDelta(t, 250.0, e.EmitLagMs, 0.001)
	require.InDelta(t, 350.0, e.E2ELagMs, 0.001)

	// sin sellos (Record) no hay latencias y los tiempos no salen en el JSON
	l.Record("r1", "m2")
	raw, err := json.Marshal(l.Snapshot(true).ByMatchID["m2"])
	require.NoError(t, err)
	require.JSONEq(t, `{"region":"r1","count":1}`, string(raw))
}

func TestHandleRecordsLatencyFromEnvelope(t *testing.T) {
	b := newBiller()
	received := time.Date(2026, 10, 16, 10, 0, 1, 0, time.UTC)
	b.Now = func() time.Time { return received }
	d := delivery(t, "r1", "m1")
	env, err := events.Unmarshal(d.Data)
	require.NoError(t, err)
	env.OccurredAt = received.Add(-900 * time.Millisecond)
	env.EmittedAt = received.Add(-400 * time.Millisecond)
	d.Data, err = events.Marshal(env)
	require.NoError(t, err)

	require.NoError(t, b.Handle(context.Background(), d))
	e := b.Ledger.Snapshot(true).ByMatchID["m1"]
	require.InDelta(t, 400.0, e.EmitLagMs, 0.001)
	require.InDelta(t, 900.0, e.E2ELagMs, 0.001)
}

func TestRegionOf(t *testing.T) {
	require.Equal(t, "r2", regionOf("energy.matched.r2.match-1", "r1"))
	require.Equal(t, "r1", regionOf("weird", "r1"))
}

func TestLoadConfig(t *testing.T) {
	_, err := LoadConfig(func(string) string { return "" })
	require.ErrorContains(t, err, "NATS_URL is required")
	c, err := LoadConfig(func(k string) string {
		return map[string]string{"NATS_URL": "nats://nats.voltera-global:4222", "NATS_USER": "billing", "NATS_PASSWORD": "p"}[k]
	})
	require.NoError(t, err)
	require.Equal(t, "BILLING_MATCHED", c.Stream)
	require.Equal(t, "billing", c.Durable)
}
