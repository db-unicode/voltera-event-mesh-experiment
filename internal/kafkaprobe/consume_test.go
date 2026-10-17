package kafkaprobe

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeConsumer entrega los lotes en orden y luego bloquea hasta que ctx se cancela.
type fakeConsumer struct {
	batches   [][]*kgo.Record
	committed []*kgo.Record
	allowed   int
}

func (f *fakeConsumer) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	if len(f.batches) == 0 {
		<-ctx.Done()
		return nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      "t",
		Partitions: []kgo.FetchPartition{{Records: b}},
	}}}}
}

func (f *fakeConsumer) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	f.committed = append(f.committed, rs...)
	return nil
}

func (f *fakeConsumer) AllowRebalance() { f.allowed++ }

func records(prefix string, from, to int) []*kgo.Record {
	rs := make([]*kgo.Record, 0, to-from)
	for n := from; n < to; n++ {
		rs = append(rs, &kgo.Record{Topic: "t", Key: []byte(fmt.Sprintf("%s-%d", prefix, n))})
	}
	return rs
}

func TestTally(t *testing.T) {
	tl := NewTally("a-")
	for _, r := range records("a", 0, 3) {
		tl.Add(r)
	}
	tl.Add(records("a", 1, 2)[0]) // duplicado
	tl.Add(records("b", 0, 1)[0]) // otro prefijo
	require.Equal(t, 4, tl.Count())
	require.Equal(t, 3, tl.Distinct())
}

func TestConsumeStopsAtExpectAndCommits(t *testing.T) {
	f := &fakeConsumer{batches: [][]*kgo.Record{records("a", 0, 5), records("a", 5, 10), records("a", 10, 15)}}
	var out bytes.Buffer
	err := Consume(t.Context(), f, ConsumeOptions{Topic: "t", Group: "g", Expect: 10, Timeout: time.Second}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "count=10 distinct=10")
	require.Len(t, f.committed, 10, "solo se confirma lo procesado")
	require.Equal(t, 2, f.allowed)
}

func TestConsumeFiltersByKeyPrefix(t *testing.T) {
	mixed := append(records("old", 0, 7), records("new", 0, 4)...)
	f := &fakeConsumer{batches: [][]*kgo.Record{mixed}}
	var out bytes.Buffer
	err := Consume(t.Context(), f, ConsumeOptions{Topic: "t", Group: "g", Expect: 4, Timeout: time.Second, KeyPrefix: "new-"}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "count=4 distinct=4")
	require.Len(t, f.committed, 11, "los eventos ajenos también avanzan el offset")
}

func TestConsumeTimeoutFailsButReportsCount(t *testing.T) {
	f := &fakeConsumer{batches: [][]*kgo.Record{records("a", 0, 3)}}
	var out bytes.Buffer
	err := Consume(t.Context(), f, ConsumeOptions{Topic: "t", Group: "g", Expect: 10, Timeout: 50 * time.Millisecond}, &out)
	require.ErrorContains(t, err, "got 3 of 10")
	require.Contains(t, out.String(), "count=3 distinct=3")
	require.Len(t, f.committed, 3)
}

func TestConsumeOptionsValidation(t *testing.T) {
	ok := ConsumeOptions{Topic: "t", Group: "g", Expect: 1, Timeout: time.Second}
	require.NoError(t, ok.validate())
	for name, mut := range map[string]func(*ConsumeOptions){
		"topic":   func(o *ConsumeOptions) { o.Topic = "" },
		"group":   func(o *ConsumeOptions) { o.Group = "" },
		"expect":  func(o *ConsumeOptions) { o.Expect = 0 },
		"timeout": func(o *ConsumeOptions) { o.Timeout = 0 },
	} {
		o := ok
		mut(&o)
		require.Error(t, o.validate(), name)
	}
}

func TestWorkCountsAndCommitsUntilCancelled(t *testing.T) {
	f := &fakeConsumer{batches: [][]*kgo.Record{records("a", 0, 4), records("a", 4, 6)}}
	opts := WorkOptions{Topic: "t", Group: "g", Delay: time.Millisecond}
	counter := newConsumedCounter(opts)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Work(ctx, f, opts, counter) }()

	require.Eventually(t, func() bool { return testutil.ToFloat64(counter) == 6 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.Len(t, f.committed, 6)
}

func TestWorkOptionsValidation(t *testing.T) {
	require.Error(t, WorkOptions{Group: "g"}.validate())
	require.Error(t, WorkOptions{Topic: "t"}.validate())
	require.Error(t, WorkOptions{Topic: "t", Group: "g", Delay: -1}.validate())
	require.NoError(t, WorkOptions{Topic: "t", Group: "g"}.validate())
}
