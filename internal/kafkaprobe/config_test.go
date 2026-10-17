package kafkaprobe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(env(nil))
	require.NoError(t, err)
	require.Equal(t, []string{"kafka.voltera-infra.svc:9092"}, cfg.Brokers)
	require.Equal(t, ":8080", cfg.MetricsAddr)
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{
		"KAFKA_BROKERS": "a:9092, b:9092,",
		"METRICS_ADDR":  "127.0.0.1:9100",
	}))
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)
	require.Equal(t, "127.0.0.1:9100", cfg.MetricsAddr)
}

func TestLoadConfigInvalid(t *testing.T) {
	_, err := LoadConfig(env(map[string]string{"KAFKA_BROKERS": "no-port"}))
	require.ErrorContains(t, err, "KAFKA_BROKERS")

	_, err = LoadConfig(env(map[string]string{"KAFKA_BROKERS": " , "}))
	require.ErrorContains(t, err, "no brokers")

	_, err = LoadConfig(env(map[string]string{"METRICS_ADDR": "8080"}))
	require.ErrorContains(t, err, "METRICS_ADDR")
}
