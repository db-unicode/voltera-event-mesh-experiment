package devicesim

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/devicecmd"
)

var key = []byte("test-key")

func newServer(t *testing.T, experiments bool) (*Server, *httptest.Server) {
	t.Helper()
	s := NewServer(key, experiments, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func post(t *testing.T, url string, body []byte, sig string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	if sig != "" {
		req.Header.Set(devicecmd.SignatureHeader, sig)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func cmd(id string) []byte {
	body, _, _ := devicecmd.Encode(key, devicecmd.Command{MatchID: id, Seller: "s", Buyer: "b", KWh: 1, IssuedAt: time.Now()})
	return body
}

func TestDuplicateCommandHasNoNewEffect(t *testing.T) {
	s, ts := newServer(t, false)
	body := cmd("m1")
	sig := devicecmd.Sign(key, body)

	code, out := post(t, ts.URL+"/commands", body, sig)
	require.Equal(t, 200, code)
	require.Equal(t, "applied", out["status"])

	code, out = post(t, ts.URL+"/commands", body, sig)
	require.Equal(t, 200, code)
	require.Equal(t, "duplicate", out["status"])

	b2 := cmd("m2")
	post(t, ts.URL+"/commands", b2, devicecmd.Sign(key, b2))
	st := s.Snapshot()
	require.Equal(t, State{Total: 3, Distinct: 2, Duplicates: 1, ByMatchID: map[string]int{"m1": 2, "m2": 1}}, st)
}

func TestBadSignatureIs401AndCounted(t *testing.T) {
	s, ts := newServer(t, false)
	body := cmd("m1")
	for _, sig := range []string{"", "zz", devicecmd.Sign([]byte("other"), body)} {
		code, _ := post(t, ts.URL+"/commands", body, sig)
		require.Equal(t, 401, code)
	}
	st := s.Snapshot()
	require.Equal(t, 3, st.Rejected)
	require.Zero(t, st.Total)
	require.Empty(t, st.ByMatchID)
}

func TestInvalidCommandIs400(t *testing.T) {
	_, ts := newServer(t, false)
	body := []byte(`{"matchId":""}`)
	code, _ := post(t, ts.URL+"/commands", body, devicecmd.Sign(key, body))
	require.Equal(t, 400, code)
}

func TestStateEndpointAndReset(t *testing.T) {
	_, ts := newServer(t, true)
	b1 := cmd("m1")
	post(t, ts.URL+"/commands", b1, devicecmd.Sign(key, b1))

	resp, err := http.Get(ts.URL + "/state")
	require.NoError(t, err)
	var st State
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
	_ = resp.Body.Close()
	require.Equal(t, 1, st.Distinct)

	resp, err = http.Post(ts.URL+"/admin/reset", "", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, 204, resp.StatusCode)
	resp, err = http.Get(ts.URL + "/state")
	require.NoError(t, err)
	st = State{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
	_ = resp.Body.Close()
	require.Zero(t, st.Total)
}

func TestResetIsNotFoundWithoutExperiments(t *testing.T) {
	_, ts := newServer(t, false)
	resp, err := http.Post(ts.URL+"/admin/reset", "", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, 404, resp.StatusCode)
}

func TestLoadConfig(t *testing.T) {
	_, err := LoadConfig(func(string) string { return "" })
	require.ErrorContains(t, err, "DEVICE_COMMAND_KEY is required")
	c, err := LoadConfig(func(k string) string {
		return map[string]string{"REGION": "r1", "DEVICE_COMMAND_KEY": "k", "EXPERIMENTS": "1"}[k]
	})
	require.NoError(t, err)
	require.True(t, c.Experiments)
	require.Equal(t, ":8080", c.HTTPAddr)
}
