package kafkaprobe

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

func TestBuildMatchedRecordsAreValidEnvelopes(t *testing.T) {
	o := MatchedOptions{Topic: "r1.energy-matched", Region: "r1", Count: 3, Prefix: "e02b-1"}
	rs, err := buildMatchedRecords(o, 0, 3, time.Unix(1700000000, 0))
	require.NoError(t, err)
	require.Len(t, rs, 3)
	ids := map[string]struct{}{}
	for i, r := range rs {
		env, err := events.Unmarshal(r.Value)
		require.NoError(t, err)
		require.Equal(t, events.TypeEnergyMatched, env.Type)
		m, err := events.DecodePayload[events.EnergyMatched](env)
		require.NoError(t, err)
		require.Equal(t, m.MatchID, string(r.Key))
		require.Equal(t, "e02b-1-s"+string(rune('0'+i)), m.SellerMeterID)
		require.NotEmpty(t, env.TraceID())
		require.Len(t, r.Headers, 3)
		ids[m.MatchID] = struct{}{}
	}
	require.Len(t, ids, 3, "matchIds distintos")
}

func TestProduceMatchedSendsAll(t *testing.T) {
	p := &fakeProducer{}
	var out bytes.Buffer
	o := MatchedOptions{Topic: "t", Region: "r1", Count: 2500, Prefix: "p"}
	require.NoError(t, ProduceMatched(t.Context(), p, o, &out))
	total := 0
	for _, b := range p.batches {
		total += len(b)
	}
	require.Equal(t, 2500, total)
	require.Contains(t, out.String(), "produced=2500 topic=t")
}

func TestMatchedOptionsValidate(t *testing.T) {
	ok := MatchedOptions{Topic: "t", Region: "r1", Count: 1, Prefix: "p"}
	require.NoError(t, ok.validate())
	bad := ok
	bad.Prefix = ""
	require.Error(t, bad.validate())
	bad = ok
	bad.Count = 0
	require.Error(t, bad.validate())
}
