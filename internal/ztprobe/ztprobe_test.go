package ztprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestParseCells(t *testing.T) {
	cells, err := ParseCells(strings.NewReader(`# comentario
a1,rabbitmq,publish,r1:meter-declarations
a2,nats,connect
a3,kafka,consume,r1.energy-matched
`))
	require.NoError(t, err)
	require.Equal(t, []Cell{
		{"a1", "rabbitmq", "publish", "r1:meter-declarations"},
		{"a2", "nats", "connect", ""},
		{"a3", "kafka", "consume", "r1.energy-matched"},
	}, cells)

	for name, in := range map[string]string{
		"broker":    "x,redis,publish,k",
		"op":        "x,kafka,subscribe,t",
		"target":    "x,kafka,publish",
		"duplicate": "x,nats,connect\nx,nats,connect",
		"fields":    "x,nats",
	} {
		_, err := ParseCells(strings.NewReader(in))
		require.Error(t, err, name)
	}
}

func TestClassify(t *testing.T) {
	for want, msgs := range map[string][]string{
		ClassAuthn:   {"nats: Authorization Violation", "Exception (403) Reason: \"ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN\""},
		ClassACL:     {"nats: Permissions Violation for Publish to \"x\"", "Exception (403) Reason: \"ACCESS_REFUSED - access to exchange 'x' in vhost 'r1' refused\""},
		ClassNetwork: {"read tcp: connection reset by peer", "EOF", "context deadline exceeded", "dial tcp: connection refused"},
		ClassOther:   {"algo raro"},
	} {
		for _, m := range msgs {
			require.Equal(t, want, Classify(errors.New(m)), m)
		}
	}
	require.Empty(t, Classify(nil))
}

const natsConf = `
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
        publish: {allow: ["$JS.API.CONSUMER.INFO.BILLING_MATCHED.billing"]}
        subscribe: {allow: ["_INBOX.>", "energy.matched.>"]}
      }}
      {user: rogue, password: rogue-pw, permissions: {publish: {deny: [">"]}, subscribe: {deny: [">"]}}}
    ]
  }
}
`

func startNATS(t *testing.T) string {
	t.Helper()
	conf := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(conf, fmt.Appendf(nil, natsConf, t.TempDir()), 0o600))
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
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "BILLING_MATCHED", Subjects: []string{"energy.matched.>"}, Storage: jetstream.MemoryStorage})
	require.NoError(t, err)
	_, err = js.CreateConsumer(ctx, "BILLING_MATCHED", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	return srv.ClientURL()
}

func natsRunner(url, user, pass string) *Runner {
	return &Runner{Env: Env{NATSURL: url, NATSUser: user, NATSPassword: pass}, Timeout: 5 * time.Second, Parallel: 4, RunID: "t"}
}

func outcomes(rs []Result) map[string]string {
	m := map[string]string{}
	for _, r := range rs {
		m[r.ID] = r.Outcome
	}
	return m
}

func TestNATSCellsFollowUserPermissions(t *testing.T) {
	url := startNATS(t)
	cells := []Cell{
		{"conn", BrokerNATS, OpConnect, ""},
		{"pub-own", BrokerNATS, OpPublish, "energy.matched.r1.x"},
		{"pub-other", BrokerNATS, OpPublish, "energy.matched.r2.x"},
		{"sub-all", BrokerNATS, OpSubscribe, "energy.matched.>"},
		{"info", BrokerNATS, OpConsumerInfo, "BILLING_MATCHED/billing"},
	}
	ctx := context.Background()

	got := outcomes(natsRunner(url, "bridge-r1", "r1-pw").Run(ctx, cells))
	require.Equal(t, map[string]string{"conn": Allowed, "pub-own": Allowed, "pub-other": Denied, "sub-all": Denied, "info": Denied}, got)

	got = outcomes(natsRunner(url, "billing", "billing-pw").Run(ctx, cells))
	require.Equal(t, map[string]string{"conn": Allowed, "pub-own": Denied, "pub-other": Denied, "sub-all": Allowed, "info": Allowed}, got)

	rogue := natsRunner(url, "rogue", "rogue-pw").Run(ctx, cells)
	require.Equal(t, map[string]string{"conn": Allowed, "pub-own": Denied, "pub-other": Denied, "sub-all": Denied, "info": Denied}, outcomes(rogue))
	for _, r := range rogue {
		if r.Outcome == Denied {
			require.Equal(t, ClassACL, r.Class, r.ID+": "+r.Error)
		}
	}

	anon := natsRunner(url, "", "").Run(ctx, cells)
	for _, r := range anon {
		require.Equal(t, Denied, r.Outcome, r.ID)
		require.Equal(t, ClassAuthn, r.Class, r.ID+": "+r.Error)
	}
}

func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestUnreachableBrokersAreDenied(t *testing.T) {
	host, port, _ := net.SplitHostPort(closedPort(t))
	portN := 0
	_, err := fmt.Sscanf(port, "%d", &portN)
	require.NoError(t, err)
	r := &Runner{Env: Env{AMQPHost: host, AMQPPort: portN, AMQPUser: "u", AMQPPassword: "p", KafkaBrokers: []string{closedPort(t)}, NATSURL: "nats://" + closedPort(t)},
		Timeout: 3 * time.Second, Parallel: 4, RunID: "t"}
	rs := r.Run(context.Background(), []Cell{
		{"r", BrokerRabbit, OpPublish, "r1:meter-declarations"},
		{"rc", BrokerRabbit, OpConsume, "r1:meter-declarations"},
		{"k", BrokerKafka, OpPublish, "r1.energy-matched"},
		{"kc", BrokerKafka, OpConsume, "r1.energy-matched"},
		{"n", BrokerNATS, OpConnect, ""},
	})
	for _, res := range rs {
		require.Equal(t, Denied, res.Outcome, res.ID)
		require.Equal(t, ClassNetwork, res.Class, res.ID+": "+res.Error)
	}
	require.Error(t, (&Runner{}).rabbit(context.Background(), Cell{Target: "sin-vhost"}))
}

func TestRunCLIWritesOneJSONLinePerCellSortedByID(t *testing.T) {
	url := startNATS(t)
	cells := "b,nats,connect\na,nats,publish,energy.matched.r2.x\n"
	env := map[string]string{"NATS_URL": url, "NATS_USER": "bridge-r1", "NATS_PASSWORD": "r1-pw"}
	var out, errb bytes.Buffer
	rc := Run(context.Background(), []string{"--timeout", "5s"}, func(k string) string { return env[k] }, strings.NewReader(cells), &out, &errb)
	require.Equal(t, 0, rc, errb.String())
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	var first, second Result
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	require.Equal(t, "a", first.ID)
	require.Equal(t, Denied, first.Outcome)
	require.Equal(t, "b", second.ID)
	require.Equal(t, Allowed, second.Outcome)

	rc = Run(context.Background(), nil, func(string) string { return "" }, strings.NewReader("x,redis,publish,k"), &out, &errb)
	require.Equal(t, 2, rc)
	rc = Run(context.Background(), nil, func(k string) string { return map[string]string{"NATS_URL": "http://x"}[k] }, strings.NewReader(""), &out, &errb)
	require.Equal(t, 2, rc)
}
