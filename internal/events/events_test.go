package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 16, 10, 0, 0, 123456789, time.UTC)

func TestNewPositionDeclaredRoundTrip(t *testing.T) {
	p := PositionDeclared{MeterID: "m-0042", Side: SideSurplus, KWh: 4.2, ValidUntil: t0.Add(time.Minute), Zone: "z1"}
	e, err := NewPositionDeclared("r1", 17, t0, p)
	require.NoError(t, err)

	id, err := uuid.Parse(e.EventID)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(7), id.Version())
	require.Equal(t, "m-0042", e.AggregateID)
	require.Equal(t, CurrentSchemaVersion, e.SchemaVersion)

	data, err := Marshal(e.WithTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"))
	require.NoError(t, err)
	got, err := Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e.EventID, got.EventID)
	require.True(t, t0.Equal(got.OccurredAt))
	require.Equal(t, "0af7651916cd43dd8448eb211c80319c", got.TraceID())

	pl, err := DecodePayload[PositionDeclared](got)
	require.NoError(t, err)
	require.Equal(t, p.MeterID, pl.MeterID)
	require.Equal(t, SideSurplus, pl.Side)
	require.InDelta(t, 4.2, pl.KWh, 1e-9)
}

func TestMarshalWireFormat(t *testing.T) {
	e, err := NewPriceUpdated("r1", 3, t0, PriceUpdated{Region: "r1", PricePerKWh: 0.21, ValidUntil: t0})
	require.NoError(t, err)
	data, err := Marshal(e)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	for _, k := range []string{"eventId", "type", "schemaVersion", "region", "aggregateId", "version", "occurredAt", "payload"} {
		require.Contains(t, m, k)
	}
	// Opcionales ausentes hasta que el relay los fije.
	for _, k := range []string{"committedAt", "emittedAt", "traceparent"} {
		require.NotContains(t, m, k)
	}
	require.Equal(t, "2026-10-16T10:00:00.123456789Z", m["occurredAt"])
	require.Equal(t, "PriceUpdated", m["type"])
}

func TestTimestampsKeepNanoseconds(t *testing.T) {
	e, err := NewEnergyMatched("r1", 1, t0, EnergyMatched{MatchID: "match-x", SellerMeterID: "s", BuyerMeterID: "b"})
	require.NoError(t, err)
	e.CommittedAt = t0.Add(time.Millisecond)
	e.EmittedAt = t0.Add(2 * time.Millisecond)
	data, err := Marshal(e)
	require.NoError(t, err)
	got, err := Unmarshal(data)
	require.NoError(t, err)
	require.True(t, e.CommittedAt.Equal(got.CommittedAt))
	require.True(t, e.EmittedAt.Equal(got.EmittedAt))
}

func TestEventIDsAreTimeOrdered(t *testing.T) {
	a, err := New(TypePriceUpdated, "r1", "r1", 1, t0, PriceUpdated{})
	require.NoError(t, err)
	time.Sleep(2 * time.Millisecond)
	b, err := New(TypePriceUpdated, "r1", "r1", 2, t0, PriceUpdated{})
	require.NoError(t, err)
	require.Less(t, a.EventID, b.EventID)
}

func TestUnmarshalRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"not json":       `{`,
		"bad uuid":       `{"eventId":"x","type":"T","schemaVersion":1,"aggregateId":"a","occurredAt":"2026-10-16T10:00:00Z","payload":{}}`,
		"no type":        `{"eventId":"` + uuid.NewString() + `","schemaVersion":1,"aggregateId":"a","occurredAt":"2026-10-16T10:00:00Z","payload":{}}`,
		"no schema":      `{"eventId":"` + uuid.NewString() + `","type":"T","aggregateId":"a","occurredAt":"2026-10-16T10:00:00Z","payload":{}}`,
		"no aggregateId": `{"eventId":"` + uuid.NewString() + `","type":"T","schemaVersion":1,"occurredAt":"2026-10-16T10:00:00Z","payload":{}}`,
		"no occurredAt":  `{"eventId":"` + uuid.NewString() + `","type":"T","schemaVersion":1,"aggregateId":"a","payload":{}}`,
		"no payload":     `{"eventId":"` + uuid.NewString() + `","type":"T","schemaVersion":1,"aggregateId":"a","occurredAt":"2026-10-16T10:00:00Z"}`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Unmarshal([]byte(in))
			require.Error(t, err)
		})
	}
}

func TestMarshalRejectsInvalid(t *testing.T) {
	_, err := Marshal(Envelope{})
	require.Error(t, err)
}

func TestMatchID(t *testing.T) {
	a := MatchID("2026-10-16T10:00:00Z", "s1", "b1")
	require.Equal(t, a, MatchID("2026-10-16T10:00:00Z", "s1", "b1"), "determinista")
	require.True(t, strings.HasPrefix(a, "match-"))
	require.Len(t, a, len("match-")+32)

	require.NotEqual(t, a, MatchID("2026-10-16T10:00:02Z", "s1", "b1"))
	require.NotEqual(t, a, MatchID("2026-10-16T10:00:00Z", "b1", "s1"), "el orden importa")
	require.NotEqual(t, MatchID("w", "ab", "c"), MatchID("w", "a", "bc"), "sin colisión por concatenación")
}

func TestPositionValidate(t *testing.T) {
	require.NoError(t, PositionDeclared{MeterID: "m", Side: SideDeficit, KWh: 1}.Validate())
	require.Error(t, PositionDeclared{Side: SideDeficit}.Validate())
	require.Error(t, PositionDeclared{MeterID: "m", Side: "X"}.Validate())
	require.Error(t, PositionDeclared{MeterID: "m", Side: SideSurplus, KWh: -1}.Validate())
}

func TestTraceID(t *testing.T) {
	require.Equal(t, "", TraceID(""))
	require.Equal(t, "", TraceID("garbage"))
}
