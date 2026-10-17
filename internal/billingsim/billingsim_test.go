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
	require.Equal(t, map[string]int{"m1": 2, "m2": 1, "m3": 1}, st.ByMatchID)
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
	require.Equal(t, map[string]int{"m1": 1}, get("/state?detail=1").ByMatchID)
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
