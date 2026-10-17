package amqpprobe

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildBody(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.FixedZone("x", 3600))
	b, err := buildBody("abcd1234", 7, now)
	require.NoError(t, err)
	var m message
	require.NoError(t, json.Unmarshal(b, &m))
	require.Equal(t, "abcd1234-7", m.EventID)
	require.Equal(t, 7, m.Seq)
	require.Equal(t, now.UTC(), m.SentAt)
}

func TestNewRunID(t *testing.T) {
	a, b := newRunID(), newRunID()
	require.Len(t, a, 8)
	require.NotEqual(t, a, b)
}

func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name        string
		exchange    string
		set         bool
		queue       string
		wantEx, key string
		wantErr     string
	}{
		{name: "exchange derivado de la cola", queue: "meter-declarations", wantEx: "meter-declarations.x", key: "meter-declarations"},
		{name: "exchange explícito", exchange: "energy-positions.x", set: true, queue: "energy-positions", wantEx: "energy-positions.x", key: "energy-positions"},
		{name: "exchange por defecto explícito", set: true, queue: "kwh-prices", wantEx: "", key: "kwh-prices"},
		{name: "sin cola", wantErr: "--queue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex, key, err := resolveTarget(tt.exchange, tt.set, tt.queue)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantEx, ex)
			require.Equal(t, tt.key, key)
		})
	}
}

func TestPublishResultOK(t *testing.T) {
	require.True(t, PublishResult{Published: 3, Confirmed: 3}.OK(3))
	require.False(t, PublishResult{Published: 3, Confirmed: 2, Nacked: 1}.OK(3))
	require.False(t, PublishResult{Published: 3, Confirmed: 3, Returned: 1}.OK(3))
	require.False(t, PublishResult{Published: 2, Confirmed: 2}.OK(3))
}

func TestClampPrefetch(t *testing.T) {
	require.Equal(t, 1, clampPrefetch(0, 100))
	require.Equal(t, 10, clampPrefetch(10, 100))
	require.Equal(t, 5, clampPrefetch(10, 5))
}

func TestPaceDelay(t *testing.T) {
	require.Zero(t, paceDelay(10, 0, 0), "sin límite")
	require.Equal(t, time.Duration(0), paceDelay(0, 50, 0), "el primero sale ya")
	require.Equal(t, 20*time.Millisecond, paceDelay(1, 50, 0))
	require.Equal(t, 10*time.Millisecond, paceDelay(1, 50, 10*time.Millisecond))
	require.Equal(t, time.Duration(0), paceDelay(1, 50, time.Second), "ya va atrasado")
	require.Equal(t, 9*time.Second, paceDelay(450, 50, 0))
}
