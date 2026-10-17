package kafkaprobe

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducer struct {
	batches [][]*kgo.Record
	err     error
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.batches = append(f.batches, rs)
	if f.err != nil {
		return kgo.ProduceResults{{Err: f.err}}
	}
	res := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		res[i] = kgo.ProduceResult{Record: r}
	}
	return res
}

func TestBuildRecords(t *testing.T) {
	rs := buildRecords("t", "p", 3, 5)
	require.Len(t, rs, 2)
	require.Equal(t, "p-3", string(rs[0].Key))
	require.Equal(t, "t", rs[0].Topic)
	require.JSONEq(t, `{"eventId":"p-4","seq":4}`, string(rs[1].Value))
}

func TestBatchPlan(t *testing.T) {
	size, pause := batchPlan(5000, 0)
	require.Equal(t, 1000, size)
	require.Zero(t, pause)

	size, pause = batchPlan(3000, 300)
	require.Equal(t, 30, size)
	require.Equal(t, 100*time.Millisecond, pause)

	size, _ = batchPlan(10, 3)
	require.Equal(t, 1, size)
}

func TestProduceSendsAllInBatches(t *testing.T) {
	p := &fakeProducer{}
	var out bytes.Buffer
	err := Produce(t.Context(), p, ProduceOptions{Topic: "t", Count: 2500, KeyPrefix: "k"}, &out)
	require.NoError(t, err)
	require.Len(t, p.batches, 3)
	total := 0
	keys := map[string]struct{}{}
	for _, b := range p.batches {
		total += len(b)
		for _, r := range b {
			keys[string(r.Key)] = struct{}{}
		}
	}
	require.Equal(t, 2500, total)
	require.Len(t, keys, 2500)
	require.Contains(t, out.String(), "produced=2500")
}

func TestProduceReturnsBrokerError(t *testing.T) {
	p := &fakeProducer{err: errors.New("boom")}
	err := Produce(t.Context(), p, ProduceOptions{Topic: "t", Count: 10, KeyPrefix: "k"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "boom")
}

func TestProduceOptionsValidation(t *testing.T) {
	for name, o := range map[string]ProduceOptions{
		"topic":  {Count: 1, KeyPrefix: "k"},
		"count":  {Topic: "t", KeyPrefix: "k"},
		"prefix": {Topic: "t", Count: 1},
		"rate":   {Topic: "t", Count: 1, KeyPrefix: "k", Rate: -1},
	} {
		require.Error(t, o.validate(), name)
	}
}
