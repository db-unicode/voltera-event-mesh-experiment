package amqpprobe

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type fakeAcker struct {
	acks   int
	ackErr error
}

func (f *fakeAcker) Ack(uint64, bool) error { f.acks++; return f.ackErr }

func (f *fakeAcker) Nack(uint64, bool, bool) error { return nil }

func (f *fakeAcker) Reject(uint64, bool) error { return nil }

func TestProcessAcksAndCounts(t *testing.T) {
	f := &fakeAcker{}
	m := &Metrics{}
	err := process(context.Background(), amqp.Delivery{Acknowledger: f, DeliveryTag: 1}, time.Millisecond, m)
	require.NoError(t, err)
	require.Equal(t, 1, f.acks)
	require.EqualValues(t, 1, m.Consumed())
}

func TestProcessCancelledDoesNotAck(t *testing.T) {
	f := &fakeAcker{}
	m := &Metrics{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := process(ctx, amqp.Delivery{Acknowledger: f}, time.Hour, m)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, f.acks)
	require.Zero(t, m.Consumed())
}

func TestProcessAckErrorDoesNotCount(t *testing.T) {
	f := &fakeAcker{ackErr: errors.New("canal cerrado")}
	m := &Metrics{}
	err := process(context.Background(), amqp.Delivery{Acknowledger: f}, 0, m)
	require.ErrorContains(t, err, "ack")
	require.Zero(t, m.Consumed())
}

func TestNextBackoff(t *testing.T) {
	d := reconnectMin
	var got []time.Duration
	for range 6 {
		got = append(got, d)
		d = nextBackoff(d)
	}
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}, got)
}
