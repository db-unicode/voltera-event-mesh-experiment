package transfer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/devicecmd"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/inbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeSender struct {
	mu   sync.Mutex
	cmds []devicecmd.Command
	err  error
}

func (f *fakeSender) Send(_ context.Context, c devicecmd.Command) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, c)
	return f.err
}

func newHandler(t *testing.T, sender Sender) (*Handler, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.Postgres(t)
	require.NoError(t, migrate.Up(context.Background(), pool, inbox.Migrations(), Migrations()))
	reg := prometheus.NewRegistry()
	svc := telemetry.New(telemetry.Info{Service: "energy-transfer", Region: "r1"}, quiet)
	return &Handler{Pool: pool, Sender: sender, Svc: svc, Metrics: telemetry.NewDomainMetrics(reg), Log: quiet}, pool
}

func record(t *testing.T, matchID string) kafkax.Record {
	t.Helper()
	env, err := events.NewEnergyMatched("r1", 1, time.Now(), events.EnergyMatched{
		MatchID: matchID, SellerMeterID: "s1", BuyerMeterID: "b1", KWh: 2.5, PricePerKWh: 0.2, Zone: "z-1", Window: "w"})
	require.NoError(t, err)
	body, err := events.Marshal(env.WithTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"))
	require.NoError(t, err)
	return kafkax.Record{Topic: "r1.energy-matched", Key: []byte(matchID), Value: body}
}

func status(t *testing.T, pool *pgxpool.Pool, matchID string) (string, *string) {
	t.Helper()
	var s string
	var e *string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT status, error FROM transfers WHERE match_id = $1`, matchID).Scan(&s, &e))
	return s, e
}

func TestFreshEventSendsCommandOnceAndMarksSent(t *testing.T) {
	sender := &fakeSender{}
	h, pool := newHandler(t, sender)
	require.NoError(t, h.Handle(context.Background(), record(t, "m1")))

	require.Len(t, sender.cmds, 1)
	require.Equal(t, devicecmd.Command{MatchID: "m1", Seller: "s1", Buyer: "b1", KWh: 2.5, IssuedAt: sender.cmds[0].IssuedAt}, sender.cmds[0])
	s, e := status(t, pool, "m1")
	require.Equal(t, StatusSent, s)
	require.Nil(t, e)
	require.Equal(t, 1.0, promtest.ToFloat64(h.Metrics.TransferCommands.WithLabelValues("sent")))

	// el mismo matchId otra vez (evento reenviado): duplicado, sin comando nuevo
	require.NoError(t, h.Handle(context.Background(), record(t, "m1")))
	require.Len(t, sender.cmds, 1)
	require.Equal(t, 1.0, promtest.ToFloat64(h.Metrics.TransferCommands.WithLabelValues("duplicate")))
	counts, err := StatusCounts(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{StatusPending: 0, StatusSent: 1, StatusFailed: 0}, counts)
}

func TestDeviceErrorMarksFailedWithoutRetry(t *testing.T) {
	sender := &fakeSender{err: errors.New("device responded 503")}
	h, pool := newHandler(t, sender)
	require.NoError(t, h.Handle(context.Background(), record(t, "m1")), "a failed command is resolved, not retried")

	s, e := status(t, pool, "m1")
	require.Equal(t, StatusFailed, s)
	require.NotNil(t, e)
	require.Contains(t, *e, "503")
	require.Equal(t, 1.0, promtest.ToFloat64(h.Metrics.TransferCommands.WithLabelValues("failed")))

	// una entrega repetida no reenvía el comando (fail-safe)
	sender.err = nil
	require.NoError(t, h.Handle(context.Background(), record(t, "m1")))
	require.Len(t, sender.cmds, 1)
}

func TestPoisonMessageIsSkipped(t *testing.T) {
	sender := &fakeSender{}
	h, pool := newHandler(t, sender)
	require.NoError(t, h.Handle(context.Background(), kafkax.Record{Topic: "t", Value: []byte("not json")}))
	other, err := events.NewPositionDeclared("r1", 1, time.Now(), events.PositionDeclared{MeterID: "m", Side: events.SideSurplus, KWh: 1, Zone: "z"})
	require.NoError(t, err)
	body, _ := events.Marshal(other)
	require.NoError(t, h.Handle(context.Background(), kafkax.Record{Topic: "t", Value: body}))
	require.Empty(t, sender.cmds)
	counts, err := StatusCounts(context.Background(), pool)
	require.NoError(t, err)
	require.Zero(t, counts[StatusPending]+counts[StatusSent]+counts[StatusFailed])
}

func TestBeforeAckFailpointFiresAfterEffect(t *testing.T) {
	sender := &fakeSender{}
	h, pool := newHandler(t, sender)
	exited := 0
	fp := failpoint.New(failpoint.WithExit(func(int) { exited++ }))
	require.NoError(t, fp.Set(FPBeforeAck, "crash-once"))
	h.FP = fp

	require.NoError(t, h.Handle(context.Background(), record(t, "m1")))
	require.Equal(t, 1, exited)
	// el efecto ya estaba aplicado y persistido cuando se "murió"
	s, _ := status(t, pool, "m1")
	require.Equal(t, StatusSent, s)
	require.Len(t, sender.cmds, 1)

	// tras el reinicio Kafka reentrega: duplicado, no se envía otro comando
	require.NoError(t, h.Handle(context.Background(), record(t, "m1")))
	require.Len(t, sender.cmds, 1)
}

func TestHTTPSenderSignsAndMapsStatus(t *testing.T) {
	key := []byte("k")
	var gotBody []byte
	var gotSig string
	code := 200
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get(devicecmd.SignatureHeader)
		w.WriteHeader(code)
	}))
	defer ts.Close()

	s := HTTPSender{BaseURL: ts.URL + "/", Key: key}
	require.NoError(t, s.Send(context.Background(), devicecmd.Command{MatchID: "m", Seller: "s", Buyer: "b", KWh: 1, IssuedAt: time.Now()}))
	require.True(t, devicecmd.Verify(key, gotBody, gotSig))

	code = 500
	err := s.Send(context.Background(), devicecmd.Command{MatchID: "m", Seller: "s", Buyer: "b", KWh: 1})
	require.ErrorContains(t, err, "500")

	ts.Close()
	require.ErrorContains(t, s.Send(context.Background(), devicecmd.Command{MatchID: "m", Seller: "s", Buyer: "b", KWh: 1}), "unreachable")
}

func TestLoadConfig(t *testing.T) {
	_, err := LoadConfig(func(string) string { return "" })
	require.ErrorContains(t, err, "DEVICE_COMMAND_KEY is required")
	c, err := LoadConfig(func(k string) string {
		return map[string]string{"REGION": "r2", "DATABASE_URL": "postgres://x", "DEVICE_SIM_URL": "http://device-sim:8080", "DEVICE_COMMAND_KEY": "k"}[k]
	})
	require.NoError(t, err)
	require.Equal(t, "r2.energy-matched", c.Topic())
	require.Equal(t, "energy-transfer", c.Group)
}
