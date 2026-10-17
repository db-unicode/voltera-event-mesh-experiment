package view

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ejecuta el probe real con TTL cortos. Ejecuta FLUSHALL: solo contra un Redis de pruebas.
func TestRunAgainstRedis(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR no definido")
	}
	if os.Getenv("TEST_REDIS_ALLOW_FLUSHALL") != "1" {
		t.Skip("TEST_REDIS_ALLOW_FLUSHALL=1 requerido: el probe ejecuta FLUSHALL")
	}
	var buf bytes.Buffer
	cfg := Config{Addr: addr, Password: os.Getenv("TEST_REDIS_PASSWORD"), TTL: 20 * time.Second, ShortTTL: 3 * time.Second, Prefix: "probe-test:"}
	failed, err := Run(context.Background(), cfg, &buf)
	require.NoError(t, err)
	require.Zero(t, failed, buf.String())
	require.Equal(t, 9, strings.Count(buf.String(), "CHECK PASS"), buf.String())
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	_, err := ConfigFromEnv()
	require.Error(t, err)
	t.Setenv("REDIS_ADDR", "x:6379")
	t.Setenv("VIEW_TTL_S", "0")
	_, err = ConfigFromEnv()
	require.Error(t, err)
	t.Setenv("VIEW_TTL_S", "150")
	c, err := ConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, 150*time.Second, c.TTL)
	require.Equal(t, 10*time.Second, c.ShortTTL)
	require.Equal(t, "s07:", c.Prefix)
}

func TestWithin(t *testing.T) {
	require.True(t, within(10500*time.Millisecond, 10*time.Second, time.Second))
	require.False(t, within(12*time.Second, 10*time.Second, time.Second))
}
