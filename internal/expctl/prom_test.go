package expctl

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func promServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var (
		mu   sync.Mutex
		seen []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Query().Get("query") == "bad(":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
		case r.URL.Path == "/api/v1/query_range":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case r.URL.Query().Get("query") == "empty":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1760000000.5,"42"]}]}}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func TestPromClient(t *testing.T) {
	srv, seen := promServer(t)
	c := &PromClient{BaseURL: srv.URL}
	raw, err := c.Query(context.Background(), "up", time.Unix(1760000000, 0))
	require.NoError(t, err)
	v, err := FirstValue(raw)
	require.NoError(t, err)
	require.Equal(t, "42", v)
	require.Contains(t, seen()[0], "time=1760000000.000")

	raw, err = c.Query(context.Background(), "empty", time.Time{})
	require.NoError(t, err)
	v, err = FirstValue(raw)
	require.NoError(t, err)
	require.Empty(t, v)

	_, err = c.Query(context.Background(), "bad(", time.Time{})
	var apiErr *promAPIError
	require.ErrorAs(t, err, &apiErr)

	_, err = c.QueryRange(context.Background(), "up", time.Unix(100, 0), time.Unix(200, 0), 1500*time.Millisecond)
	require.NoError(t, err)
	require.Contains(t, seen()[len(seen())-1], "step=1.5")
}

func TestCmdPromQueryAndRange(t *testing.T) {
	srv, _ := promServer(t)
	dir := t.TempDir()
	var out, errb bytes.Buffer
	require.Equal(t, 0, Run(context.Background(), []string{"prom", "query", "up", "--url", srv.URL}, &out, &errb, &fakeKube{}), errb.String())
	require.Contains(t, out.String(), `"status":"success"`)

	f := filepath.Join(dir, "r.json")
	require.Equal(t, 0, Run(context.Background(), []string{"prom", "range", "up", "--start", "-1m", "--step", "5s", "--url", srv.URL, "--out", f}, &out, &errb, &fakeKube{}), errb.String())
	raw, err := os.ReadFile(f)
	require.NoError(t, err)
	require.Contains(t, string(raw), "matrix")

	require.Equal(t, 1, Run(context.Background(), []string{"prom", "query", "bad(", "--url", srv.URL}, &out, &errb, &fakeKube{}))
	require.Equal(t, 2, Run(context.Background(), []string{"prom", "range", "up", "--url", srv.URL}, &out, &errb, &fakeKube{}))
}

func TestPromSessionReopensForwardOnTransportError(t *testing.T) {
	srv, _ := promServer(t)
	s := &PromSession{Kube: &fakeKube{forwardTo: srv}}
	defer s.Close()
	calls := 0
	raw, err := s.Do(context.Background(), func(c *PromClient) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, http.ErrHandlerTimeout
		}
		return c.Query(context.Background(), "up", time.Time{})
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.NotEmpty(t, raw)
}
