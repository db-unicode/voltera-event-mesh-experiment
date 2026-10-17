package expctl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPercentile(t *testing.T) {
	vals := []float64{4, 1, 3, 2, 5}
	for q, want := range map[float64]float64{0: 1, 0.5: 3, 1: 5, 0.25: 2, 0.9: 4.6} {
		got, err := Percentile(vals, q)
		require.NoError(t, err)
		require.InDelta(t, want, got, 1e-9, "q=%v", q)
	}
	_, err := Percentile(nil, 0.5)
	require.Error(t, err)
	_, err = Percentile(vals, 1.5)
	require.Error(t, err)
	require.Equal(t, []float64{4, 1, 3, 2, 5}, vals, "no debe reordenar la entrada")
}

func TestApplyFunc(t *testing.T) {
	v := []float64{3, 1, 4, 1, 5}
	cases := map[string]float64{"max": 5, "min": 1, "first": 3, "last": 5, "count": 5, "sum": 14, "avg": 2.8, "p50": 3, "p100": 5}
	for fn, want := range cases {
		got, err := ApplyFunc(fn, v)
		require.NoError(t, err, fn)
		require.InDelta(t, want, got, 1e-9, fn)
	}
	_, err := ApplyFunc("median", v)
	require.Error(t, err)
	_, err = ApplyFunc("max", nil)
	require.Error(t, err)
	n, err := ApplyFunc("count", nil)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestTableFloatsSkipsEmpty(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.csv")
	require.NoError(t, os.WriteFile(p, []byte("ts,a,b\n1,1,x\n2,,y\n3,3,z\n"), 0o644))
	tb, err := ReadCSV(p)
	require.NoError(t, err)
	got, err := tb.Floats("a")
	require.NoError(t, err)
	require.Equal(t, []float64{1, 3}, got)
	_, err = tb.Floats("b")
	require.Error(t, err)
	_, err = tb.Floats("nope")
	require.Error(t, err)
}
