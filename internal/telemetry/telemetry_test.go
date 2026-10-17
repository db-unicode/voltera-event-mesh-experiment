package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
)

func testEnvelope(t *testing.T) events.Envelope {
	t.Helper()
	e, err := events.NewPositionDeclared("r1", 1, time.Now().Add(-2*time.Second),
		events.PositionDeclared{MeterID: "m-1", Side: events.SideSurplus, KWh: 1, Zone: "z"})
	require.NoError(t, err)
	return e.WithTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
}

func TestLoggerFields(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger("gateway", &buf, slog.LevelInfo)
	env := testEnvelope(t)
	LogStage(l, StagePublish, env, ResultOK, "topic", "energy-positions")

	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	require.Equal(t, "gateway", m["service"])
	require.Equal(t, env.EventID, m["eventId"])
	require.Equal(t, "0af7651916cd43dd8448eb211c80319c", m["traceId"])
	require.Equal(t, "publish", m["stage"])
	require.Equal(t, "ok", m["result"])
	require.Equal(t, "energy-positions", m["topic"])
	ts, err := time.Parse(time.RFC3339Nano, m["ts"].(string))
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), ts, 5*time.Second)
	require.NotContains(t, m, "time")
	require.Equal(t, "info", m["level"])
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestMetricsEndpointExposesCommonMetrics(t *testing.T) {
	s := New(Info{Service: "probe", Region: "r1"}, nil)
	env := testEnvelope(t)
	s.Published(env, "rabbit")
	s.Consumed(env, "rabbit", ResultOK)
	s.Consumed(env, "rabbit", ResultDuplicate)
	s.Consumed(env, "rabbit", ResultStale)
	s.Metrics.ObserveLag(env.WithTraceparent(""), time.Now())
	s.Metrics.ProcessingSeconds.WithLabelValues(env.Type).Observe(0.01)

	body := get(t, s.Handler(), "/metrics").Body.String()
	for _, want := range []string{
		`events_published_total{channel="rabbit",event_type="PositionDeclared",service="probe"} 1`,
		`events_consumed_total{channel="rabbit",event_type="PositionDeclared",result="ok",service="probe"} 1`,
		`events_consumed_total{channel="rabbit",event_type="PositionDeclared",result="duplicate",service="probe"} 1`,
		`events_duplicates_dropped_total{event_type="PositionDeclared",service="probe"} 1`,
		`events_stale_dropped_total{event_type="PositionDeclared",service="probe"} 1`,
		`event_e2e_lag_seconds_bucket{event_type="PositionDeclared",service="probe",stage="occurred",le="120"} 1`,
		`event_e2e_lag_seconds_bucket{event_type="PositionDeclared",service="probe",stage="occurred",le="0.01"} 0`,
		`event_processing_seconds_count{event_type="PositionDeclared",service="probe"} 1`,
		`go_goroutines{service="probe"}`,
	} {
		require.Contains(t, body, want)
	}
	// CommittedAt/EmittedAt son cero: no se observan.
	require.NotContains(t, body, `stage="committed"`)
}

func TestDomainMetricsRegister(t *testing.T) {
	s := New(Info{Service: "x"}, nil)
	d := NewDomainMetrics(s.Reg)
	d.DeclarationsProcessed.Inc()
	d.MatchesCreated.WithLabelValues("r1").Inc()
	body := get(t, s.Handler(), "/metrics").Body.String()
	require.Contains(t, body, `declarations_processed_total{service="x"} 1`)
	require.Contains(t, body, `matches_created_total{region="r1",service="x"} 1`)
}

func TestHealthzAndReadyz(t *testing.T) {
	s := New(Info{Service: "probe"}, nil)
	h := s.Handler()
	require.Equal(t, http.StatusOK, get(t, h, "/healthz").Code)
	require.Equal(t, http.StatusOK, get(t, h, "/readyz").Code)

	healthy := true
	s.AddReadyCheck("postgres", func(context.Context) error {
		if healthy {
			return nil
		}
		return errors.New("down")
	})
	require.Equal(t, http.StatusOK, get(t, h, "/readyz").Code)
	healthy = false
	w := get(t, h, "/readyz")
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"postgres":"down"`)
	require.Equal(t, http.StatusOK, get(t, h, "/healthz").Code, "liveness no depende de los checks")

	healthy = true
	s.SetNotReady(true)
	require.Equal(t, http.StatusServiceUnavailable, get(t, h, "/readyz").Code)
}

func TestDebugState(t *testing.T) {
	s := New(Info{Service: "projector", Version: "abc", Region: "r1", Instance: "pod-0"}, nil)
	s.Failpoints = failpoint.New()
	require.NoError(t, s.Failpoints.Set("drop_position", "drop"))
	s.AddState("outbox", func(context.Context) any { return map[string]any{"pending": 3, "oldestAgeSec": 1.5} })
	env := testEnvelope(t)
	s.Consumed(env, "rabbit", ResultOK)
	s.Consumed(env, "rabbit", ResultDuplicate)

	var st map[string]any
	require.NoError(t, json.Unmarshal(get(t, s.Handler(), "/debug/state").Body.Bytes(), &st))
	require.Equal(t, "projector", st["service"])
	require.Equal(t, "pod-0", st["instance"])
	require.Equal(t, env.EventID, st["lastEventId"])
	require.Contains(t, st, "startedAt")
	require.Contains(t, st, "lastEventAt")
	require.Equal(t, map[string]any{"ok": 1.0, "duplicate": 1.0, "stale": 0.0, "error": 0.0}, st["consumed"])
	require.Equal(t, map[string]any{"pending": 3.0, "oldestAgeSec": 1.5}, st["outbox"])
	require.Equal(t, map[string]any{"drop_position": "drop"}, st["failpoints"])
}

func TestAdminFailpointOnlyWithExperiments(t *testing.T) {
	post := func(h http.Handler) int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/failpoint?name=a&mode=drop", nil))
		return w.Code
	}
	s := New(Info{Service: "x"}, nil)
	s.Failpoints = failpoint.New()
	s.Experiments = false
	require.Equal(t, http.StatusNotFound, post(s.Handler()))
	require.Empty(t, s.Failpoints.Snapshot())

	s.Experiments = true
	require.Equal(t, http.StatusOK, post(s.Handler()))
	require.Equal(t, "drop", s.Failpoints.Snapshot()["a"])
}

func TestServeShutsDownOnCancel(t *testing.T) {
	s := New(Info{Service: "x"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, "127.0.0.1:0") }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Serve no terminó")
	}
}
