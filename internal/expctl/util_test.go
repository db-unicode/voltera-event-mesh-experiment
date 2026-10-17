package expctl

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Time{
		"now":                            now,
		"-5m":                            now.Add(-5 * time.Minute),
		"2026-10-16T10:00:00.5Z":         time.Date(2026, 10, 16, 10, 0, 0, 500_000_000, time.UTC),
		"1760616000":                     time.Unix(1760616000, 0).UTC(),
		"2026-10-16T10:00:00.123456789Z": time.Date(2026, 10, 16, 10, 0, 0, 123456789, time.UTC),
	}
	for in, want := range cases {
		got, err := ParseTime(in, now)
		require.NoError(t, err, in)
		require.True(t, want.Equal(got), "%s: %v != %v", in, want, got)
	}
	_, err := ParseTime("ayer", now)
	require.Error(t, err)
}

func TestNowStringIsFixedWidthUTC(t *testing.T) {
	s := NowString()
	require.Len(t, s, len("2026-10-16T10:00:00.123456789Z"))
	_, err := time.Parse(time.RFC3339Nano, s)
	require.NoError(t, err)
}

func TestParseHTTPTarget(t *testing.T) {
	got, err := parseHTTPTarget("voltera-r1/device-sim:9090/state")
	require.NoError(t, err)
	require.Equal(t, httpTarget{Namespace: "voltera-r1", Resource: "svc/device-sim", Port: 9090, Path: "/state"}, got)

	got, err = parseHTTPTarget("voltera-r1/pod/gw-abc:9090/admin/failpoint?name=x&mode=off")
	require.NoError(t, err)
	require.Equal(t, httpTarget{Namespace: "voltera-r1", Resource: "pod/gw-abc", Port: 9090, Path: "/admin/failpoint?name=x&mode=off"}, got)

	got, err = parseHTTPTarget("ns/svc:80")
	require.NoError(t, err)
	require.Equal(t, "/", got.Path)

	for _, bad := range []string{"svc:9090/x", "ns/svc/x", "ns/svc:abc/x", "ns/:80/x"} {
		_, err := parseHTTPTarget(bad)
		require.Error(t, err, bad)
	}
}

func TestParseReplicaSpec(t *testing.T) {
	rs, err := parseReplicaSpec("voltera-infra/kafka-exporter")
	require.NoError(t, err)
	require.Equal(t, replicaSpec{Name: "kafka-exporter", Namespace: "voltera-infra", Resource: "deployment/kafka-exporter"}, rs)

	rs, err = parseReplicaSpec("gw=voltera-r1/statefulset/positions-db")
	require.NoError(t, err)
	require.Equal(t, replicaSpec{Name: "gw", Namespace: "voltera-r1", Resource: "statefulset/positions-db"}, rs)

	_, err = parseReplicaSpec("solo")
	require.Error(t, err)
}

func TestRunUsageAndUnknown(t *testing.T) {
	var out, errb bytes.Buffer
	require.Equal(t, 2, Run(context.Background(), nil, &out, &errb, &fakeKube{}))
	require.Equal(t, 2, Run(context.Background(), []string{"nope"}, &out, &errb, &fakeKube{}))
	out.Reset()
	require.Equal(t, 0, Run(context.Background(), []string{"now"}, &out, &errb, &fakeKube{}))
	require.Contains(t, out.String(), "T")
}

func TestParseStep(t *testing.T) {
	d, err := parseStep("1s")
	require.NoError(t, err)
	require.Equal(t, time.Second, d)
	d, err = parseStep("0.5")
	require.NoError(t, err)
	require.Equal(t, 500*time.Millisecond, d)
	_, err = parseStep("0")
	require.Error(t, err)
}

func TestElapsed(t *testing.T) {
	var out, errb bytes.Buffer
	code := Run(context.Background(), []string{"elapsed", "2026-10-16T10:00:00.250000000Z", "2026-10-16T10:00:03.750000000Z"}, &out, &errb, &fakeKube{})
	require.Equal(t, 0, code, errb.String())
	require.Equal(t, "3.500\n", out.String())
	require.Equal(t, 2, Run(context.Background(), []string{"elapsed"}, &out, &errb, &fakeKube{}))
}

func TestReservePortInRange(t *testing.T) {
	seen := map[int]bool{}
	for range 5 {
		p, err := reservePort()
		require.NoError(t, err)
		require.GreaterOrEqual(t, p, forwardPortMin)
		require.LessOrEqual(t, p, forwardPortMax)
		require.False(t, seen[p], "puerto repetido")
		seen[p] = true
	}
	for p := range seen {
		releasePort(p)
	}
}
