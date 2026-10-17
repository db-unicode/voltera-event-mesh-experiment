package kafkaprobe

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRejectsBadInput(t *testing.T) {
	noEnv := func(string) string { return "" }
	for name, args := range map[string][]string{
		"no args":            nil,
		"unknown subcommand": {"nope"},
		"produce no topic":   {"produce", "--count", "1", "--key-prefix", "k"},
		"consume no expect":  {"consume", "--topic", "t", "--group", "g"},
		"work bad delay":     {"work", "--topic", "t", "--group", "g", "--delay", "-1s"},
		"seek bad target":    {"seek", "--topic", "t", "--group", "g", "--to", "middle"},
		"unknown flag":       {"produce", "--nope"},
	} {
		require.Error(t, Run(t.Context(), args, noEnv, &bytes.Buffer{}), name)
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	env := func(k string) string {
		if k == "KAFKA_BROKERS" {
			return "broken"
		}
		return ""
	}
	err := Run(t.Context(), []string{"produce"}, env, &bytes.Buffer{})
	require.ErrorContains(t, err, "KAFKA_BROKERS")
}
