package expctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const podsJSON = `{"items":[
 {"metadata":{"name":"gw-1"},"spec":{"containers":[{"name":"app"},{"name":"istio-proxy"}]},"status":{"containerStatuses":[{"name":"app","restartCount":1},{"name":"istio-proxy","restartCount":0}]}},
 {"metadata":{"name":"proj-1"},"spec":{"containers":[{"name":"app"}]},"status":{"containerStatuses":[{"name":"app","restartCount":0}]}}
]}`

func traceKube() *fakeKube {
	return &fakeKube{handler: func(args []string) ([]byte, error) {
		ns := args[1]
		switch {
		case hasArgs(args, "get", "pods"):
			if ns == "voltera-r1" {
				return []byte(podsJSON), nil
			}
			return []byte(`{"items":[]}`), nil
		case hasArgs(args, "logs", "gw-1", "-c", "app") && hasArgs(args, "--previous"):
			return []byte("2026-10-16T10:00:00.000000100Z {\"ts\":\"2026-10-16T10:00:00.000000100Z\",\"service\":\"declaration-gateway\",\"eventId\":\"ev-1\",\"stage\":\"commit\",\"result\":\"ok\"}\n"), nil
		case hasArgs(args, "logs", "gw-1", "-c", "app"):
			return []byte("2026-10-16T10:00:00.5Z texto plano ev-1\n" +
				"2026-10-16T10:00:01.000000000Z {\"ts\":\"2026-10-16T10:00:01.000000000Z\",\"service\":\"declaration-gateway\",\"eventId\":\"ev-1\",\"traceId\":\"t1\",\"stage\":\"publish\",\"result\":\"ok\"}\n" +
				"2026-10-16T10:00:03.000000000Z {\"ts\":\"2026-10-16T10:00:03.000000000Z\",\"service\":\"declaration-gateway\",\"eventId\":\"otro\",\"stage\":\"publish\"}\n"), nil
		case hasArgs(args, "logs", "proj-1", "-c", "app"):
			return []byte("2026-10-16T10:00:02.250000000Z {\"service\":\"market-projector\",\"msg\":\"applied aggregate m-1 ev-1\",\"stage\":\"apply\",\"result\":\"ok\"}\n{roto\n"), nil
		}
		return nil, errors.New("inesperado: " + strings.Join(args, " "))
	}}
}

func TestTracerCollectSortsAndFilters(t *testing.T) {
	var warn bytes.Buffer
	tr := &Tracer{Kube: traceKube(), Namespaces: []string{"voltera-r1", "voltera-r2"}, Since: "10m", IDs: []string{"ev-1"}, Warn: &warn}
	hops, err := tr.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, hops, 3, "previous + publish + línea del proyector (id dentro del mensaje)")
	require.Equal(t, []string{"commit", "publish", "apply"}, []string{hops[0].Stage, hops[1].Stage, hops[2].Stage})
	require.Equal(t, "market-projector", hops[2].Service, "ts de kubectl cuando la línea no trae ts")
	require.InDelta(t, 0, hops[0].DeltaMs, 1e-9)
	require.InDelta(t, 1000, hops[1].OffsetMs, 0.001)
	require.InDelta(t, 1250, hops[2].DeltaMs, 0.001)
	require.InDelta(t, 2250, hops[2].OffsetMs, 0.001)
	require.Contains(t, warn.String(), "ignoradas")
	for _, h := range hops {
		require.NotEqual(t, "istio-proxy", h.Container)
	}
}

func TestCmdTraceOutputs(t *testing.T) {
	dir := t.TempDir()
	var out, errb bytes.Buffer
	file := dir + "/trace.json"
	code := Run(context.Background(), []string{"trace", "ev-1", "--namespaces", "voltera-r1", "--out", file}, &out, &errb, traceKube())
	require.Equal(t, 0, code, errb.String())
	require.Contains(t, out.String(), "ts")
	require.Contains(t, out.String(), "declaration-gateway")
	require.Contains(t, out.String(), "gw-1")

	out.Reset()
	code = Run(context.Background(), []string{"trace", "--json", "ev-1", "--namespaces", "voltera-r1"}, &out, &errb, traceKube())
	require.Equal(t, 0, code)
	var res struct {
		Count    int      `json:"count"`
		Services []string `json:"services"`
		TotalMs  float64  `json:"totalMs"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &res))
	require.Equal(t, 3, res.Count)
	require.Equal(t, []string{"declaration-gateway", "market-projector"}, res.Services)
	require.InDelta(t, 2250, res.TotalMs, 0.001)

	require.Equal(t, 2, Run(context.Background(), []string{"trace"}, &out, &errb, traceKube()))
}

func TestParseLogLine(t *testing.T) {
	_, _, _, ok := ParseLogLine("sin-espacios")
	require.False(t, ok)
	_, _, _, ok = ParseLogLine("2026-10-16T10:00:00Z hola mundo")
	require.False(t, ok)
	ts, f, _, ok := ParseLogLine(`2026-10-16T10:00:00Z {"a":"b"}`)
	require.True(t, ok)
	require.Equal(t, "b", f["a"])
	require.Equal(t, 2026, ts.Year())
}
