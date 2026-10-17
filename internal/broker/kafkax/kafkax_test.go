package kafkax

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
)

func TestRecordHeadersAndKey(t *testing.T) {
	r := ProducerRecord(outbox.Message{EventID: "e1", Type: "EnergyMatched", Topic: "r1.energy-matched", Key: "match-1",
		Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", Body: []byte(`{"a":1}`)})
	require.Equal(t, "r1.energy-matched", r.Topic)
	require.Equal(t, "match-1", string(r.Key))
	require.Equal(t, `{"a":1}`, string(r.Value))
	got := map[string]string{}
	for _, h := range r.Headers {
		got[h.Key] = string(h.Value)
	}
	require.Equal(t, map[string]string{
		"eventId": "e1", "type": "EnergyMatched",
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}, got)

	// sin traceparent no se añade el header
	r = ProducerRecord(outbox.Message{EventID: "e2", Type: "T", Topic: "t", Key: "k", Body: []byte("{}")})
	require.Len(t, r.Headers, 2)
}

func TestNewPublisherAndConsumerValidate(t *testing.T) {
	_, err := NewPublisher(PublisherConfig{})
	require.ErrorContains(t, err, "no brokers")
	_, err = NewConsumer(ConsumerConfig{Brokers: []string{"x:9092"}})
	require.ErrorContains(t, err, "required")
}

// fakeClient entrega lotes preparados y registra los commits.
type fakeClient struct {
	mu      sync.Mutex
	batches []kgo.Fetches
	commits []int64 // offsets confirmados, en orden
	rebal   int
	cancel  context.CancelFunc
}

func (f *fakeClient) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batches) == 0 {
		f.cancel()
		return nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b
}

func (f *fakeClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rs {
		f.commits = append(f.commits, r.Offset)
	}
	return nil
}
func (f *fakeClient) AllowRebalance()            { f.mu.Lock(); f.rebal++; f.mu.Unlock() }
func (f *fakeClient) Ping(context.Context) error { return nil }
func (f *fakeClient) Close()                     {}

func fetch(part int32, offsets ...int64) kgo.Fetches {
	rs := make([]*kgo.Record, len(offsets))
	for i, o := range offsets {
		rs[i] = &kgo.Record{Topic: "t", Partition: part, Offset: o, Key: []byte("k"),
			Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("tp")}}}
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: part, Records: rs}}}}}}
}

func run(t *testing.T, fc *fakeClient, h Handler) *Consumer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	fc.cancel = cancel
	c := newConsumer(fc, ConsumerConfig{Group: "g", Topic: "t", InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond,
		Registerer: prometheus.NewRegistry()})
	require.NoError(t, c.Run(ctx, h))
	return c
}

func TestConsumerCommitsAfterHandlerInOrder(t *testing.T) {
	fc := &fakeClient{batches: []kgo.Fetches{fetch(0, 1, 2, 3)}}
	var seen []int64
	c := run(t, fc, func(_ context.Context, r Record) error {
		// al ejecutar el handler del offset n aún no se confirmó n
		require.NotContains(t, fc.commits, r.Offset)
		require.Equal(t, "tp", r.Headers["traceparent"])
		seen = append(seen, r.Offset)
		return nil
	})
	require.Equal(t, []int64{1, 2, 3}, seen)
	require.Equal(t, []int64{1, 2, 3}, fc.commits)
	require.Positive(t, fc.rebal)
	require.Equal(t, 3.0, promtest.ToFloat64(c.handled.WithLabelValues("ok")))
}

func TestConsumerRetriesSameMessageWithoutCommit(t *testing.T) {
	fc := &fakeClient{batches: []kgo.Fetches{fetch(0, 10, 11)}}
	attempts := map[int64]int{}
	c := run(t, fc, func(_ context.Context, r Record) error {
		attempts[r.Offset]++
		if r.Offset == 10 && attempts[10] < 3 {
			require.Empty(t, fc.commits, "no commit while the handler fails")
			return errors.New("boom")
		}
		return nil
	})
	require.Equal(t, 3, attempts[10])
	require.Equal(t, 1, attempts[11])
	require.Equal(t, []int64{10, 11}, fc.commits)
	require.Equal(t, 2.0, promtest.ToFloat64(c.handled.WithLabelValues("error")))
}

func TestConsumerStopsRetryingOnShutdownWithoutCommit(t *testing.T) {
	fc := &fakeClient{batches: []kgo.Fetches{fetch(0, 5)}}
	ctx, cancel := context.WithCancel(context.Background())
	fc.cancel = cancel
	c := newConsumer(fc, ConsumerConfig{Group: "g", Topic: "t", InitialBackoff: time.Millisecond, Registerer: prometheus.NewRegistry()})
	calls := 0
	require.NoError(t, c.Run(ctx, func(context.Context, Record) error {
		calls++
		cancel()
		return errors.New("still failing")
	}))
	require.Equal(t, 1, calls)
	require.Empty(t, fc.commits)
}
