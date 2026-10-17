package natsx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const testConf = `
port: -1
host: 127.0.0.1
jetstream { store_dir: %q }
accounts {
  MESH {
    jetstream: enabled
    users: [
      {user: admin, password: admin-pw}
      {user: bridge-r1, password: r1-pw, permissions: {publish: {allow: ["energy.matched.r1.>"]}, subscribe: {allow: ["_INBOX.>"]}}}
      {user: billing, password: billing-pw, permissions: {
        publish: {allow: ["$JS.API.CONSUMER.INFO.BILLING_MATCHED.billing", "$JS.API.CONSUMER.MSG.NEXT.BILLING_MATCHED.billing", "$JS.ACK.BILLING_MATCHED.billing.>"]}
        subscribe: {allow: ["_INBOX.>", "energy.matched.>"]}
      }}
    ]
  }
}
`

func startServer(t *testing.T) string {
	t.Helper()
	conf := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(conf, []byte(fmt.Sprintf(testConf, t.TempDir())), 0o600))
	opts, err := server.ProcessConfigFile(conf)
	require.NoError(t, err)
	opts.NoLog, opts.NoSigs = true, true
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(10*time.Second))

	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "admin-pw"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "BILLING_MATCHED", Subjects: []string{"energy.matched.>"},
		Duplicates: 2 * time.Minute, Storage: jetstream.MemoryStorage})
	require.NoError(t, err)
	_, err = js.CreateConsumer(ctx, "BILLING_MATCHED", jetstream.ConsumerConfig{Durable: "billing",
		AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "energy.matched.>", AckWait: 500 * time.Millisecond})
	require.NoError(t, err)
	return srv.ClientURL()
}

func connect(t *testing.T, url, user, pass string) *Client {
	t.Helper()
	c, err := Connect(Config{URL: url, User: user, Password: pass, Name: "test"})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestConnectValidates(t *testing.T) {
	_, err := Connect(Config{URL: "nats://x:4222", User: "u"})
	require.ErrorContains(t, err, "required")
	_, err = Connect(Config{URL: "http://x", User: "u", Password: "p"})
	require.ErrorContains(t, err, "nats://")
}

func TestPublishReturnsOnlyWithPubAckAndDedupes(t *testing.T) {
	url := startServer(t)
	pub := NewPublisher(connect(t, url, "bridge-r1", "r1-pw"), 2*time.Second)
	ctx := context.Background()

	ack, err := pub.Publish(ctx, Msg{Subject: "energy.matched.r1.m1", MsgID: "m1", Header: map[string]string{"traceparent": "tp"}, Data: []byte(`{}`)})
	require.NoError(t, err)
	require.Equal(t, "BILLING_MATCHED", ack.Stream)
	require.False(t, ack.Duplicate)

	// mismo Nats-Msg-Id dentro de la ventana: el stream lo descarta pero confirma
	ack, err = pub.Publish(ctx, Msg{Subject: "energy.matched.r1.m1", MsgID: "m1", Data: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, ack.Duplicate)

	_, err = pub.Publish(ctx, Msg{Subject: "energy.matched.r1.x"})
	require.ErrorContains(t, err, "required")
}

func TestPublishPermissionViolationIsRejected(t *testing.T) {
	url := startServer(t)
	pub := NewPublisher(connect(t, url, "bridge-r1", "r1-pw"), 500*time.Millisecond)
	_, err := pub.Publish(context.Background(), Msg{Subject: "energy.matched.r2.m1", MsgID: "m1", Data: []byte(`{}`)})
	var rej *RejectedError
	require.ErrorAs(t, err, &rej)
	require.Equal(t, ReasonPermission, rej.Reason)
}

func TestPublishOnClosedConnectionIsNoAck(t *testing.T) {
	url := startServer(t)
	c := connect(t, url, "bridge-r1", "r1-pw")
	pub := NewPublisher(c, 300*time.Millisecond)
	c.nc.Close()
	_, err := pub.Publish(context.Background(), Msg{Subject: "energy.matched.r1.m2", MsgID: "m2", Data: []byte(`{}`)})
	var rej *RejectedError
	require.ErrorAs(t, err, &rej)
	require.Equal(t, ReasonNoAck, rej.Reason)
	require.Error(t, c.Healthy())
}

func TestPullConsumerAcksNaksAndRedelivers(t *testing.T) {
	url := startServer(t)
	pub := NewPublisher(connect(t, url, "bridge-r1", "r1-pw"), 2*time.Second)
	for _, id := range []string{"a", "b"} {
		_, err := pub.Publish(context.Background(), Msg{Subject: "energy.matched.r1." + id, MsgID: id, Header: map[string]string{"traceparent": "tp-" + id}, Data: []byte(id)})
		require.NoError(t, err)
	}

	pc, err := NewPullConsumer(context.Background(), connect(t, url, "billing", "billing-pw"),
		PullConfig{Stream: "BILLING_MATCHED", Durable: "billing", MaxWait: 300 * time.Millisecond, NakDelay: 50 * time.Millisecond})
	require.NoError(t, err)

	var mu sync.Mutex
	attempts := map[string]int{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = pc.Run(ctx, func(_ context.Context, d Delivery) error {
			mu.Lock()
			defer mu.Unlock()
			id := string(d.Data)
			attempts[id]++
			require.Equal(t, "tp-"+id, d.Header.Get("traceparent"))
			if id == "a" && attempts[id] < 3 {
				return errors.New("transient")
			}
			return nil
		})
	}()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts["a"] == 3 && attempts["b"] == 1
	}, 10*time.Second, 50*time.Millisecond)
	// confirmados: pasado el ack_wait no hay reentregas
	time.Sleep(900 * time.Millisecond)
	mu.Lock()
	require.Equal(t, 3, attempts["a"])
	require.Equal(t, 1, attempts["b"])
	mu.Unlock()
	cancel()
	<-done
}

func TestNewPullConsumerMissingDurable(t *testing.T) {
	url := startServer(t)
	_, err := NewPullConsumer(context.Background(), connect(t, url, "admin", "admin-pw"), PullConfig{Stream: "BILLING_MATCHED", Durable: "nope"})
	require.Error(t, err)
}
