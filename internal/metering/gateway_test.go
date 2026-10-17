package metering

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// memStore implementa Store en memoria con la misma regla de versiones que Postgres.
type memStore struct {
	mu       sync.Mutex
	versions map[string]int64
	events   []events.Envelope
	err      error
}

func (m *memStore) Apply(_ context.Context, env events.Envelope, p events.PositionDeclared) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if m.versions[p.MeterID] >= env.Version {
		return false, nil
	}
	m.versions[p.MeterID] = env.Version
	m.events = append(m.events, env)
	return true, nil
}

type rig struct {
	gw    *Gateway
	store *memStore
	svc   *telemetry.Service
	wakes int
}

func newRig(t *testing.T) *rig {
	t.Helper()
	a, err := acl.New(4)
	require.NoError(t, err)
	svc := telemetry.New(telemetry.Info{Service: ServiceName, Region: "r1"}, slog.New(slog.DiscardHandler))
	r := &rig{store: &memStore{versions: map[string]int64{}}, svc: svc}
	r.gw = &Gateway{Region: "r1", ACL: a, Store: r.store, Svc: svc, Domain: telemetry.NewDomainMetrics(svc.Reg),
		Rejected: NewRejectedCounter(svc.Reg), Wake: func() { r.wakes++ }}
	return r
}

func delivery(vendor, body string, hdr amqp.Table) amqpx.Delivery {
	h := amqp.Table{"x-vendor": vendor}
	for k, v := range hdr {
		h[k] = v
	}
	return amqpx.Delivery{Queue: QueueDeclarations, Body: []byte(body), Headers: h}
}

func acmeBody(meter string, seq int, ts time.Time) string {
	return `{"meter":"` + meter + `","mode":"SELL","energyWh":4200,"horizonSec":120,"seq":` + itoa(seq) + `,"ts":"` + ts.Format(time.RFC3339Nano) + `"}`
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestAppliedDeclarationCreatesEventAndWakesRelay(t *testing.T) {
	r := newRig(t)
	tp := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	id := "0199aaaa-bbbb-7ccc-8ddd-eeeeffff0001"
	err := r.gw.Handle(context.Background(), delivery(acl.VendorAcme, acmeBody("m-0001", 17, time.Now()),
		amqp.Table{"traceparent": tp, "eventId": id}))
	require.NoError(t, err)

	require.Len(t, r.store.events, 1)
	env := r.store.events[0]
	require.Equal(t, id, env.EventID, "el eventId de la declaración identifica al evento de dominio")
	require.Equal(t, events.TypePositionDeclared, env.Type)
	require.Equal(t, int64(17), env.Version)
	require.Equal(t, tp, env.Traceparent)
	require.Equal(t, "r1", env.Region)
	p, err := events.DecodePayload[events.PositionDeclared](env)
	require.NoError(t, err)
	require.Equal(t, events.SideSurplus, p.Side)
	require.Equal(t, 4.2, p.KWh)
	require.Equal(t, "z-2", p.Zone)

	require.Equal(t, 1, r.wakes)
	require.Equal(t, 1.0, promtest.ToFloat64(r.gw.Domain.DeclarationsProcessed))
	require.Equal(t, 1.0, promtest.ToFloat64(r.svc.Metrics.EventsConsumed.WithLabelValues(events.TypePositionDeclared, QueueDeclarations, "ok")))
}

func TestInvalidEventIDHeaderGetsFreshID(t *testing.T) {
	r := newRig(t)
	require.NoError(t, r.gw.Handle(context.Background(), delivery(acl.VendorAcme, acmeBody("m-0001", 1, time.Now()), amqp.Table{"eventId": "not-a-uuid"})))
	require.NotEqual(t, "not-a-uuid", r.store.events[0].EventID)
	require.NoError(t, r.store.events[0].Validate())
}

func TestDuplicateAndOldVersionsAreStaleWithoutEvent(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	require.NoError(t, r.gw.Handle(ctx, delivery(acl.VendorAcme, acmeBody("m-0001", 5, time.Now()), nil)))
	require.NoError(t, r.gw.Handle(ctx, delivery(acl.VendorAcme, acmeBody("m-0001", 5, time.Now()), nil))) // duplicado
	require.NoError(t, r.gw.Handle(ctx, delivery(acl.VendorAcme, acmeBody("m-0001", 4, time.Now()), nil))) // viejo

	require.Len(t, r.store.events, 1, "solo la declaración aplicada genera outbox")
	require.Equal(t, 1, r.wakes)
	require.Equal(t, 2.0, promtest.ToFloat64(r.svc.Metrics.StaleDropped.WithLabelValues(events.TypePositionDeclared)))
	require.Equal(t, 3.0, promtest.ToFloat64(r.gw.Domain.DeclarationsProcessed), "se procesaron las tres")
}

func TestRejectedDeclarationsAreInvalidAndCounted(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cases := []struct {
		vendor, body, wantVendor, wantReason string
	}{
		{"mystery-v3", acmeBody("m-0001", 1, time.Now()), "unknown", acl.ReasonUnknownVendor},
		{"", acmeBody("m-0001", 1, time.Now()), "unknown", acl.ReasonUnknownVendor},
		{acl.VendorAcme, "no-es-json", acl.VendorAcme, acl.ReasonMalformed},
		{acl.VendorVoltix, acmeBody("m-0001", 1, time.Now()), acl.VendorVoltix, acl.ReasonInvalid},
	}
	for _, c := range cases {
		before := promtest.ToFloat64(r.gw.Rejected.WithLabelValues(c.wantVendor, c.wantReason))
		err := r.gw.Handle(ctx, delivery(c.vendor, c.body, nil))
		var ie *amqpx.InvalidError
		require.ErrorAs(t, err, &ie, "se rechaza sin reintento: %s/%s", c.vendor, c.wantReason)
		require.Equal(t, before+1, promtest.ToFloat64(r.gw.Rejected.WithLabelValues(c.wantVendor, c.wantReason)))
	}
	require.Empty(t, r.store.events)
	require.Zero(t, r.wakes)
	require.Zero(t, promtest.ToFloat64(r.gw.Domain.DeclarationsProcessed))
}

func TestStoreErrorIsTransient(t *testing.T) {
	r := newRig(t)
	r.store.err = errors.New("connection refused")
	err := r.gw.Handle(context.Background(), delivery(acl.VendorAcme, acmeBody("m-0001", 1, time.Now()), nil))
	require.Error(t, err)
	var ie *amqpx.InvalidError
	require.False(t, errors.As(err, &ie), "un fallo de la base se reintenta: no es un mensaje inválido")
	require.Zero(t, r.wakes)
	require.Zero(t, promtest.ToFloat64(r.gw.Domain.DeclarationsProcessed))
}

func TestProcessingDelayAndCancellation(t *testing.T) {
	r := newRig(t)
	r.gw.Delay = 50 * time.Millisecond
	start := time.Now()
	require.NoError(t, r.gw.Handle(context.Background(), delivery(acl.VendorAcme, acmeBody("m-0001", 1, time.Now()), nil)))
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, r.gw.Handle(ctx, delivery(acl.VendorAcme, acmeBody("m-0002", 1, time.Now()), nil)), context.Canceled)
}

func TestConfigFromEnv(t *testing.T) {
	base := map[string]string{"REGION": "r1", "DATABASE_URL": "postgres://x", "AMQP_USER": "u", "AMQP_PASSWORD": "p"}
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	c, err := ConfigFromEnv(get(base))
	require.NoError(t, err)
	require.Equal(t, 20*time.Millisecond, c.ProcessingDelay)
	require.Equal(t, 4, c.Zones)
	require.Equal(t, 10, c.Prefetch)
	require.Equal(t, "r1", c.AMQP.Vhost)
	require.Equal(t, ":9090", c.OpsAddr)

	_, err = ConfigFromEnv(get(map[string]string{"PROCESSING_DELAY_MS": "-3", "ZONES": "0"}))
	require.ErrorContains(t, err, "REGION")
	require.ErrorContains(t, err, "DATABASE_URL")
	require.ErrorContains(t, err, "PROCESSING_DELAY_MS")
	require.ErrorContains(t, err, "ZONES")
	require.ErrorContains(t, err, "AMQP_USER")
}
