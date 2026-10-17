package amqpprobe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		c, err := LoadConfig(env(map[string]string{"AMQP_USER": "u", "AMQP_PASSWORD": "p"}))
		require.NoError(t, err)
		require.Equal(t, "rabbitmq.voltera-infra.svc", c.Host)
		require.Equal(t, 5672, c.Port)
		require.Empty(t, c.Vhost)
	})
	t.Run("overrides", func(t *testing.T) {
		c, err := LoadConfig(env(map[string]string{
			"AMQP_USER": "u", "AMQP_PASSWORD": "p", "AMQP_HOST": "h", "AMQP_PORT": "5673", "AMQP_VHOST": "r1",
		}))
		require.NoError(t, err)
		require.Equal(t, Config{Host: "h", Port: 5673, User: "u", Password: "p", Vhost: "r1"}, c)
	})
	t.Run("faltan credenciales", func(t *testing.T) {
		_, err := LoadConfig(env(nil))
		require.ErrorContains(t, err, "AMQP_USER")
		require.ErrorContains(t, err, "AMQP_PASSWORD")
	})
	t.Run("puerto inválido", func(t *testing.T) {
		for _, p := range []string{"abc", "0", "70000"} {
			_, err := LoadConfig(env(map[string]string{"AMQP_USER": "u", "AMQP_PASSWORD": "p", "AMQP_PORT": p}))
			require.ErrorContains(t, err, "AMQP_PORT", p)
		}
	})
}

func TestConfigURI(t *testing.T) {
	c := Config{Host: "rabbitmq", Port: 5672, User: "meter-sim-r1", Password: "p@ss/word"}
	require.Equal(t, "amqp://meter-sim-r1:p%40ss%2Fword@rabbitmq/r1", c.URI("r1"))
	require.Equal(t, "amqp://meter-sim-r1:p%40ss%2Fword@rabbitmq/", c.URI("/"))
	c.Port = 5673
	require.Equal(t, "amqp://meter-sim-r1:p%40ss%2Fword@rabbitmq:5673/r2", c.URI("r2"))
}
