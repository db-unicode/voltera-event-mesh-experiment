package meshprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const testStream = "BILLING_MATCHED"

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
      {user: bridge-r2, password: r2-pw, permissions: {publish: {allow: ["energy.matched.r2.>"]}, subscribe: {allow: ["_INBOX.>"]}}}
      {user: billing, password: billing-pw, permissions: {
        publish: {allow: ["$JS.API.CONSUMER.INFO.BILLING_MATCHED.billing", "$JS.API.CONSUMER.MSG.NEXT.BILLING_MATCHED.billing", "$JS.ACK.BILLING_MATCHED.billing.>"]}
        subscribe: {allow: ["_INBOX.>", "energy.matched.>"]}
      }}
      {user: rogue, password: rogue-pw, permissions: {publish: {deny: [">"]}, subscribe: {deny: [">"]}}}
    ]
  }
}
`

// startServer levanta un nodo con los mismos usuarios y permisos que el nodo global del cluster.
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
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       testStream,
		Subjects:   []string{"energy.matched.>"},
		Duplicates: 2 * time.Minute,
		Storage:    jetstream.MemoryStorage,
	})
	require.NoError(t, err)
	_, err = js.CreateConsumer(ctx, testStream, jetstream.ConsumerConfig{
		Durable:       "billing",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "energy.matched.>",
	})
	require.NoError(t, err)
	return srv.ClientURL()
}

func connect(t *testing.T, url, user, pass string) *Client {
	t.Helper()
	c, err := Connect(Conn{URL: url, User: user, Password: pass})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestConnFromEnv(t *testing.T) {
	env := map[string]string{"NATS_URL": "nats://x:4222", "NATS_USER": "u", "NATS_PASSWORD": "p"}
	get := func(k string) string { return env[k] }

	c, err := ConnFromEnv(get)
	require.NoError(t, err)
	require.Equal(t, Conn{URL: "nats://x:4222", User: "u", Password: "p"}, c)

	delete(env, "NATS_PASSWORD")
	_, err = ConnFromEnv(get)
	require.ErrorContains(t, err, "NATS_PASSWORD")

	env["NATS_PASSWORD"] = "p"
	env["NATS_URL"] = "http://x"
	_, err = ConnFromEnv(get)
	require.ErrorContains(t, err, "nats://")
}

func TestPublishAckAndSubscribeDistinct(t *testing.T) {
	url := startServer(t)
	ctx := context.Background()

	r1 := connect(t, url, "bridge-r1", "r1-pw").Publish(ctx, PublishOptions{
		Subject: "energy.matched.r1.a", Count: 10, MsgIDPrefix: "r1", AckTimeout: 2 * time.Second,
	})
	require.Equal(t, 10, r1.Acks)
	require.Zero(t, r1.Errors)
	require.Equal(t, []string{testStream}, r1.Streams)
	require.Equal(t, uint64(10), r1.LastSeq)

	r2 := connect(t, url, "bridge-r2", "r2-pw").Publish(ctx, PublishOptions{
		Subject: "energy.matched.r2.a", Count: 10, MsgIDPrefix: "r2", AckTimeout: 2 * time.Second,
	})
	require.Equal(t, 10, r2.Acks)

	sub := connect(t, url, "billing", "billing-pw").Subscribe(ctx, SubscribeOptions{
		Stream: testStream, Durable: "billing", Expect: 20, Timeout: 10 * time.Second, Linger: 300 * time.Millisecond,
	})
	require.Empty(t, sub.FirstError)
	require.Equal(t, 20, sub.Received)
	require.Equal(t, 20, sub.Distinct)
	require.Equal(t, map[string]int{"energy.matched.r1": 10, "energy.matched.r2": 10}, sub.ByPrefix)
}

func TestPublishDuplicateIsDeduplicated(t *testing.T) {
	url := startServer(t)
	res := connect(t, url, "bridge-r1", "r1-pw").Publish(context.Background(), PublishOptions{
		Subject: "energy.matched.r1.a", Count: 5, MsgIDPrefix: "dup", Repeat: 2, AckTimeout: 2 * time.Second,
	})
	require.Equal(t, 10, res.Acks)
	require.Equal(t, 5, res.DuplicateAcks)

	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin-pw"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	st, err := js.Stream(context.Background(), testStream)
	require.NoError(t, err)
	info, err := st.Info(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(5), info.State.Msgs)
}

func TestPublishCrossRegionIsRejected(t *testing.T) {
	url := startServer(t)
	res := connect(t, url, "bridge-r2", "r2-pw").Publish(context.Background(), PublishOptions{
		Subject: "energy.matched.r1.x", Count: 2, MsgIDPrefix: "cross", AckTimeout: time.Second,
	})
	require.Zero(t, res.Acks)
	require.Equal(t, 2, res.Errors)
	require.NotZero(t, res.Violations)
	require.Contains(t, res.ViolationMsgs[0], "Permissions Violation for Publish")
}

func TestRogueCannotPublishNorSubscribe(t *testing.T) {
	url := startServer(t)
	ctx := context.Background()
	rogue := connect(t, url, "rogue", "rogue-pw")

	pub := rogue.Publish(ctx, PublishOptions{Subject: "energy.matched.r1.x", Count: 1, MsgIDPrefix: "rogue", AckTimeout: time.Second})
	require.Zero(t, pub.Acks)
	require.NotZero(t, pub.Violations)

	sub := connect(t, url, "rogue", "rogue-pw").Subscribe(ctx, SubscribeOptions{Core: true, Subject: "energy.matched.>", Timeout: 2 * time.Second})
	require.Zero(t, sub.Received)
	require.NotZero(t, sub.Violations)
	require.Contains(t, sub.ViolationMsgs[0], "Permissions Violation for Subscription")
}

func TestPublishWithoutStreamNeverAcks(t *testing.T) {
	url := startServer(t)
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin-pw"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	require.NoError(t, js.DeleteStream(context.Background(), testStream))

	res := connect(t, url, "bridge-r1", "r1-pw").Publish(context.Background(), PublishOptions{
		Subject: "energy.matched.r1.a", Count: 3, MsgIDPrefix: "y", AckTimeout: time.Second,
	})
	require.Zero(t, res.Acks)
	require.Equal(t, 3, res.Errors)
}

func TestPublishRetriesUntilStreamReturns(t *testing.T) {
	url := startServer(t)
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin-pw"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, js.DeleteStream(ctx, testStream))

	go func() {
		time.Sleep(700 * time.Millisecond)
		_, _ = js.CreateStream(ctx, jetstream.StreamConfig{
			Name: testStream, Subjects: []string{"energy.matched.>"}, Storage: jetstream.MemoryStorage,
		})
	}()
	res := connect(t, url, "bridge-r1", "r1-pw").Publish(ctx, PublishOptions{
		Subject: "energy.matched.r1.a", Count: 1, MsgIDPrefix: "retry", AckTimeout: time.Second, RetryTimeout: 10 * time.Second,
	})
	require.Equal(t, 1, res.Acks)
	require.NotZero(t, res.Retries)
	require.GreaterOrEqual(t, res.FirstAckMS, int64(500))
}

func TestRunValidatesFlagsAndEnv(t *testing.T) {
	var out bytes.Buffer
	noEnv := func(string) string { return "" }
	require.Equal(t, 2, Run(context.Background(), nil, noEnv, &out))
	require.Equal(t, 2, Run(context.Background(), []string{"publish", "--subject", "a.b"}, noEnv, &out))
	require.Equal(t, 2, Run(context.Background(), []string{"publish", "--subject", "a.b", "--msg-id-prefix", "p"}, noEnv, &out))
	require.Equal(t, 2, Run(context.Background(), []string{"subscribe"}, noEnv, &out))
}

func TestRunPublishPrintsResultLine(t *testing.T) {
	url := startServer(t)
	env := map[string]string{"NATS_URL": url, "NATS_USER": "bridge-r1", "NATS_PASSWORD": "r1-pw"}
	var out bytes.Buffer
	code := Run(context.Background(),
		[]string{"publish", "--subject", "energy.matched.r1.a", "--count", "3", "--msg-id-prefix", "cli"},
		func(k string) string { return env[k] }, &out)
	require.Equal(t, 0, code)

	var line struct {
		Msg    string        `json:"msg"`
		Result PublishResult `json:"result"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &line))
	require.Equal(t, "result", line.Msg)
	require.Equal(t, 3, line.Result.Acks)
}
