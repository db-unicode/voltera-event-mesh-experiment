package envcfg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderAccumulatesErrors(t *testing.T) {
	env := map[string]string{"REGION": "r3", "N": "x", "F": "-1", "OK": "7", "I64": "12"}
	r := New(func(k string) string { return env[k] })
	require.Equal(t, "d", r.String("MISSING", "d"))
	require.Equal(t, 7, r.Int("OK", 1, 0))
	require.Equal(t, 3, r.Int("UNSET", 3, 0))
	require.Equal(t, int64(12), r.Int64("I64", 0))
	require.NoError(t, r.Err())

	r.Region()
	r.Required("NOPE")
	r.Int("N", 1, 0)
	r.Float("F", 1, 0)
	r.Int("OK", 1, 8)
	err := r.Err()
	require.ErrorContains(t, err, "REGION")
	require.ErrorContains(t, err, "NOPE is required")
	require.ErrorContains(t, err, "N must be an integer")
	require.ErrorContains(t, err, "F must be a number")
	require.ErrorContains(t, err, "OK must be an integer >= 8")
}

func TestRegion(t *testing.T) {
	r := New(func(string) string { return "r2" })
	require.Equal(t, "r2", r.Region())
	require.NoError(t, r.Err())
}
