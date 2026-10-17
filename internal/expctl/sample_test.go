package expctl

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSamplerRun(t *testing.T) {
	srv, _ := promServer(t)
	kube := &fakeKube{handler: func(args []string) ([]byte, error) {
		switch {
		case hasArgs(args, "-n", "ns1", "get", "deployment/a"):
			return []byte("3/2"), nil
		case hasArgs(args, "-n", "ns1", "get", "deployment/zero"):
			return []byte("0/"), nil
		}
		return nil, errors.New("boom")
	}}
	var warn bytes.Buffer
	s := &Sampler{
		Kube:     kube,
		Prom:     &PromSession{BaseURL: srv.URL},
		Replicas: []replicaSpec{{"a", "ns1", "deployment/a"}, {"zero", "ns1", "deployment/zero"}, {"bad", "ns1", "deployment/bad"}},
		Proms:    []promCol{{"up", "up"}, {"empty", "empty"}},
		Warn:     &warn,
	}
	var out bytes.Buffer
	n, err := s.Run(context.Background(), &out, 20*time.Millisecond, 70*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 2)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Equal(t, "ts,a,a_ready,zero,zero_ready,bad,bad_ready,up,empty", lines[0])
	require.Len(t, lines, n+1)
	cols := strings.Split(lines[1], ",")
	require.Equal(t, []string{"3", "2", "0", "0", "", "", "42", ""}, cols[1:])
	_, err = time.Parse(time.RFC3339Nano, cols[0])
	require.NoError(t, err)
	require.Contains(t, warn.String(), "bad")
}

func TestSamplerStopsOnContextCancel(t *testing.T) {
	kube := &fakeKube{handler: func([]string) ([]byte, error) { return []byte("1/1"), nil }}
	s := &Sampler{Kube: kube, Replicas: []replicaSpec{{"a", "ns", "deployment/a"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	var out bytes.Buffer
	n, err := s.Run(ctx, &out, 20*time.Millisecond, time.Hour)
	require.NoError(t, err)
	require.Positive(t, n)
}

func TestCmdSampleValidation(t *testing.T) {
	var out, errb bytes.Buffer
	run := func(args ...string) int {
		return Run(context.Background(), append([]string{"sample"}, args...), &out, &errb, &fakeKube{})
	}
	require.Equal(t, 2, run("--out", "x.csv"))
	require.Equal(t, 2, run("--replicas", "x", "--out", "x.csv"))
	require.Equal(t, 2, run("--prom", "novalue", "--out", "x.csv"))
	require.Equal(t, 2, run("--replicas", "n/a", "--replicas", "m/a", "--out", "x.csv"), "columnas duplicadas")
}
