package kafkaprobe

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricsHandlerExposesCounterAndHealthz(t *testing.T) {
	counter := newConsumedCounter(WorkOptions{Topic: "r1.energy-matched", Group: "g"})
	reg, err := newRegistry(counter)
	require.NoError(t, err)
	counter.Add(7)

	srv := httptest.NewServer(newMetricsHandler(reg))
	defer srv.Close()

	body := get(t, srv.URL+"/metrics")
	require.Contains(t, body, `events_consumed_total{group="g",topic="r1.energy-matched"} 7`)

	require.Equal(t, "ok\n", get(t, srv.URL+"/healthz"))
}

func get(t *testing.T, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	buf := make([]byte, 1<<20)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}
