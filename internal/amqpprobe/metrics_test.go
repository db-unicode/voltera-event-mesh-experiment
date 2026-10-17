package amqpprobe

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestMetricsEndpoint(t *testing.T) {
	m := &Metrics{}
	m.IncConsumed()
	m.IncConsumed()
	rec := get(t, m.Handler(), "/metrics")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "# TYPE events_consumed_total counter\n")
	require.Contains(t, rec.Body.String(), "events_consumed_total 2\n")
}

func TestHealthAndReadiness(t *testing.T) {
	m := &Metrics{}
	require.Equal(t, http.StatusOK, get(t, m.Handler(), "/healthz").Code)
	require.Equal(t, http.StatusServiceUnavailable, get(t, m.Handler(), "/readyz").Code)
	m.SetConnected(true)
	require.Equal(t, http.StatusOK, get(t, m.Handler(), "/readyz").Code)
}
