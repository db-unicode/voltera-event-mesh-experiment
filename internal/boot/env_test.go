package boot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func envOf(m map[string]string) *Env { return NewEnv(func(k string) string { return m[k] }) }

func TestEnvDefaultsAndRequired(t *testing.T) {
	e := envOf(map[string]string{"REGION": "r2", "N": "7", "S": "3"})
	require.Equal(t, "r2", e.Region())
	require.Equal(t, 7, e.Int("N", 1, 1))
	require.Equal(t, 5, e.Int("MISSING", 5, 1))
	require.Equal(t, 3*time.Second, e.Seconds("S", 1, 1))
	require.Equal(t, ":9090", e.Addr("OPS_ADDR", ":9090"))
	require.Equal(t, []string{"kafka:9092"}, e.HostPorts("BROKERS", "kafka:9092"))
	require.NoError(t, e.Err())
}

func TestEnvAccumulatesErrors(t *testing.T) {
	e := envOf(map[string]string{"REGION": "r9", "N": "0", "BROKERS": "nohostport", "OPS_ADDR": "x"})
	e.Region()
	e.Int("N", 1, 1)
	e.Required("DATABASE_URL")
	e.HostPorts("BROKERS", "")
	e.Addr("OPS_ADDR", ":9090")
	err := e.Err()
	require.ErrorContains(t, err, "REGION must be r1 or r2")
	require.ErrorContains(t, err, "N must be an integer >= 1")
	require.ErrorContains(t, err, "DATABASE_URL is required")
	require.ErrorContains(t, err, "BROKERS: invalid address")
	require.ErrorContains(t, err, "OPS_ADDR: invalid address")
}
