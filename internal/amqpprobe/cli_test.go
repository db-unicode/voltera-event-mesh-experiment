package amqpprobe

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParsePublish(t *testing.T) {
	cfg := Config{Vhost: "r1"}
	t.Run("vhost del entorno y exchange derivado", func(t *testing.T) {
		a, err := parsePublish(cfg, []string{"--queue", "meter-declarations", "--count", "2000"}, &bytes.Buffer{})
		require.NoError(t, err)
		require.Equal(t, "r1", a.vhost)
		require.Equal(t, PublishOptions{Exchange: "meter-declarations.x", RoutingKey: "meter-declarations", Count: 2000}, a.opts)
	})
	t.Run("exchange por defecto explícito", func(t *testing.T) {
		a, err := parsePublish(cfg, []string{"--queue", "q", "--exchange="}, &bytes.Buffer{})
		require.NoError(t, err)
		require.Empty(t, a.opts.Exchange)
	})
	t.Run("flag vhost manda sobre el entorno", func(t *testing.T) {
		a, err := parsePublish(cfg, []string{"--vhost", "r2", "--queue", "q"}, &bytes.Buffer{})
		require.NoError(t, err)
		require.Equal(t, "r2", a.vhost)
	})
	t.Run("sin cola", func(t *testing.T) {
		_, err := parsePublish(cfg, nil, &bytes.Buffer{})
		require.ErrorIs(t, err, errUsage)
	})
	t.Run("sin vhost", func(t *testing.T) {
		_, err := parsePublish(Config{}, []string{"--queue", "q"}, &bytes.Buffer{})
		require.ErrorIs(t, err, errUsage)
	})
}

func TestParseConsume(t *testing.T) {
	cfg := Config{Vhost: "r1"}
	a, err := parseConsume(cfg, []string{"--queue", "q", "--expect", "5", "--prefetch", "3"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, ConsumeOptions{Queue: "q", Expect: 5, Prefetch: 3}, a.opts)

	_, err = parseConsume(cfg, []string{"--queue", "q"}, &bytes.Buffer{})
	require.ErrorIs(t, err, errUsage)
}

func TestParseWork(t *testing.T) {
	cfg := Config{Vhost: "r1"}
	a, err := parseWork(cfg, []string{"--queue", "q"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, WorkOptions{Queue: "q", Prefetch: 10, Delay: 20 * time.Millisecond}, a.opts)
	require.Equal(t, ":8080", a.listen)

	_, err = parseWork(cfg, []string{"--queue", "q", "--prefetch", "0"}, &bytes.Buffer{})
	require.ErrorIs(t, err, errUsage)
}

func TestRunExitCodes(t *testing.T) {
	envOK := env(map[string]string{"AMQP_USER": "u", "AMQP_PASSWORD": "p"})
	run := func(args []string, getenv func(string) string) int {
		return Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, getenv)
	}
	require.Equal(t, 2, run(nil, envOK), "sin subcomando")
	require.Equal(t, 2, run([]string{"publish"}, env(nil)), "sin credenciales")
	require.Equal(t, 2, run([]string{"bogus"}, envOK), "subcomando desconocido")
	require.Equal(t, 2, run([]string{"publish", "--vhost", "r1"}, envOK), "sin cola")
	require.Equal(t, 2, run([]string{"consume", "--vhost", "r1", "--queue", "q"}, envOK), "sin expect")
	require.Equal(t, 0, run([]string{"help"}, envOK))
}
