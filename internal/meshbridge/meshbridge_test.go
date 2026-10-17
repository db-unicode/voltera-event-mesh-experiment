package meshbridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/natsx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newBridge(pub PublishFunc) *Bridge {
	return &Bridge{Region: "r1", Publish: pub, Log: quiet,
		Svc:     telemetry.New(telemetry.Info{Service: "mesh-bridge", Region: "r1"}, quiet),
		Metrics: telemetry.NewDomainMetrics(prometheus.NewRegistry())}
}

func record(t *testing.T, matchID string) kafkax.Record {
	t.Helper()
	env, err := events.NewEnergyMatched("r1", 1, time.Now(), events.EnergyMatched{MatchID: matchID, SellerMeterID: "s", BuyerMeterID: "b", KWh: 1, Zone: "z", Window: "w"})
	require.NoError(t, err)
	body, err := events.Marshal(env)
	require.NoError(t, err)
	return kafkax.Record{Topic: "r1.energy-matched", Value: body,
		Headers: map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}
}

func TestHandlePublishesSubjectMsgIDAndTraceparent(t *testing.T) {
	var got natsx.Msg
	b := newBridge(func(_ context.Context, m natsx.Msg) error { got = m; return nil })
	rec := record(t, "match-abc")
	require.NoError(t, b.Handle(context.Background(), rec))

	require.Equal(t, "energy.matched.r1.match-abc", got.Subject)
	require.Equal(t, "match-abc", got.MsgID)
	require.Equal(t, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", got.Header["traceparent"])
	require.Equal(t, rec.Value, got.Data)
	require.Equal(t, 1.0, promtest.ToFloat64(b.Metrics.MeshPublished.WithLabelValues("r1")))
}

func TestHandleReturnsErrorWithoutAckSoOffsetIsNotCommitted(t *testing.T) {
	b := newBridge(func(context.Context, natsx.Msg) error {
		return &natsx.RejectedError{Reason: natsx.ReasonNoAck, Err: errors.New("timeout")}
	})
	err := b.Handle(context.Background(), record(t, "match-abc"))
	require.Error(t, err)
	require.Equal(t, 1.0, promtest.ToFloat64(b.Metrics.MeshPublishRejected.WithLabelValues("r1", "no_ack")))
	require.Zero(t, promtest.ToFloat64(b.Metrics.MeshPublished.WithLabelValues("r1")))

	// un error que no es RejectedError también cuenta como rechazo
	b = newBridge(func(context.Context, natsx.Msg) error { return errors.New("x") })
	require.Error(t, b.Handle(context.Background(), record(t, "match-abc")))
	require.Equal(t, 1.0, promtest.ToFloat64(b.Metrics.MeshPublishRejected.WithLabelValues("r1", "other")))
}

func TestHandleSkipsPoisonMessages(t *testing.T) {
	called := false
	b := newBridge(func(context.Context, natsx.Msg) error { called = true; return nil })
	require.NoError(t, b.Handle(context.Background(), kafkax.Record{Value: []byte("nope")}))
	require.NoError(t, b.Handle(context.Background(), record(t, "bad.match.id")))
	require.NoError(t, b.Handle(context.Background(), record(t, "bad>")))
	require.False(t, called)
}

func TestLoadConfig(t *testing.T) {
	env := map[string]string{"REGION": "r2", "NATS_URL": "nats://nats.voltera-r2:4222", "NATS_USER": "bridge-r2", "NATS_PASSWORD": "p"}
	c, err := LoadConfig(func(k string) string { return env[k] })
	require.NoError(t, err)
	require.Equal(t, "r2.energy-matched", c.Topic())
	require.Equal(t, 5*time.Second, c.AckTimeout)

	env["NATS_URL"] = "http://x"
	_, err = LoadConfig(func(k string) string { return env[k] })
	require.ErrorContains(t, err, "nats://")
	delete(env, "NATS_USER")
	_, err = LoadConfig(func(k string) string { return env[k] })
	require.ErrorContains(t, err, "NATS_USER is required")
}
