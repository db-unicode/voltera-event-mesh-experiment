package acl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

const (
	acmeOK   = `{"meter":"m-0001","mode":"SELL","energyWh":4200,"horizonSec":120,"seq":17,"ts":"2026-10-16T10:00:00.123456789Z"}`
	voltixOK = `{"id":"m-0002","position":{"type":"deficit","kwh":3.1},"validity":{"until":"2026-10-16T10:02:00.5Z"},"version":9,"zoneCode":"Z3","measuredAt":"2026-10-16T10:00:00.25Z"}`
)

func mustACL(t *testing.T, zones int) *ACL {
	t.Helper()
	a, err := New(zones)
	require.NoError(t, err)
	return a
}

func TestAcmeTranslation(t *testing.T) {
	d, err := mustACL(t, 4).Translate(VendorAcme, []byte(acmeOK))
	require.NoError(t, err)
	ts := time.Date(2026, 10, 16, 10, 0, 0, 123456789, time.UTC)
	require.Equal(t, events.PositionDeclared{MeterID: "m-0001", Side: events.SideSurplus, KWh: 4.2,
		ValidUntil: ts.Add(120 * time.Second), Zone: "z-2"}, d.Position)
	require.Equal(t, int64(17), d.Version)
	require.True(t, d.OccurredAt.Equal(ts))
}

func TestVoltixTranslation(t *testing.T) {
	d, err := mustACL(t, 4).Translate(VendorVoltix, []byte(voltixOK))
	require.NoError(t, err)
	require.Equal(t, events.PositionDeclared{MeterID: "m-0002", Side: events.SideDeficit, KWh: 3.1,
		ValidUntil: time.Date(2026, 10, 16, 10, 2, 0, 500_000_000, time.UTC), Zone: "z-3"}, d.Position)
	require.Equal(t, int64(9), d.Version)
	require.True(t, d.OccurredAt.Equal(time.Date(2026, 10, 16, 10, 0, 0, 250_000_000, time.UTC)))
}

func TestBothVendorsProduceTheSameDomainEvent(t *testing.T) {
	a := mustACL(t, 4)
	acme, err := a.Translate(VendorAcme, []byte(`{"meter":"m-0003","mode":"BUY","energyWh":2500,"horizonSec":60,"seq":1,"ts":"2026-10-16T10:00:00Z"}`))
	require.NoError(t, err)
	voltix, err := a.Translate(VendorVoltix, []byte(`{"id":"m-0003","position":{"type":"DEFICIT","kwh":2.5},"validity":{"until":"2026-10-16T10:01:00Z"},"version":1,"zoneCode":"Z`+
		ZoneOf("m-0003", 4)[2:]+`","measuredAt":"2026-10-16T10:00:00Z"}`))
	require.NoError(t, err)
	require.Equal(t, acme, voltix, "el dominio no distingue el fabricante")
}

func TestRejections(t *testing.T) {
	a := mustACL(t, 4)
	cases := []struct {
		name, vendor, body, reason string
	}{
		{"unknown vendor", "other-v9", acmeOK, ReasonUnknownVendor},
		{"empty vendor", "", acmeOK, ReasonUnknownVendor},
		{"acme body with voltix vendor", VendorVoltix, acmeOK, ReasonInvalid},
		{"voltix body with acme vendor", VendorAcme, voltixOK, ReasonInvalid},
		{"not json", VendorAcme, `not-json`, ReasonMalformed},
		{"trailing data", VendorAcme, acmeOK + `{}`, ReasonMalformed},
		{"wrong type", VendorAcme, `{"meter":1}`, ReasonMalformed},
		{"acme bad mode", VendorAcme, `{"meter":"m-1","mode":"HOLD","energyWh":1,"horizonSec":60,"seq":1,"ts":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"acme negative energy", VendorAcme, `{"meter":"m-1","mode":"BUY","energyWh":-1,"horizonSec":60,"seq":1,"ts":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"acme zero horizon", VendorAcme, `{"meter":"m-1","mode":"BUY","energyWh":1,"horizonSec":0,"seq":1,"ts":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"acme seq zero", VendorAcme, `{"meter":"m-1","mode":"BUY","energyWh":1,"horizonSec":60,"seq":0,"ts":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"acme bad ts", VendorAcme, `{"meter":"m-1","mode":"BUY","energyWh":1,"horizonSec":60,"seq":1,"ts":"yesterday"}`, ReasonInvalid},
		{"acme bad meter id", VendorAcme, `{"meter":"m:1*","mode":"BUY","energyWh":1,"horizonSec":60,"seq":1,"ts":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"voltix bad zone", VendorVoltix, `{"id":"m-2","position":{"type":"surplus","kwh":1},"validity":{"until":"2026-10-16T10:01:00Z"},"version":1,"zoneCode":"3","measuredAt":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"voltix bad type", VendorVoltix, `{"id":"m-2","position":{"type":"flat","kwh":1},"validity":{"until":"2026-10-16T10:01:00Z"},"version":1,"zoneCode":"Z1","measuredAt":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"voltix expired before measure", VendorVoltix, `{"id":"m-2","position":{"type":"surplus","kwh":1},"validity":{"until":"2026-10-16T09:00:00Z"},"version":1,"zoneCode":"Z1","measuredAt":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"voltix negative kwh", VendorVoltix, `{"id":"m-2","position":{"type":"surplus","kwh":-1},"validity":{"until":"2026-10-16T10:01:00Z"},"version":1,"zoneCode":"Z1","measuredAt":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
		{"voltix missing field", VendorVoltix, `{"id":"m-2"}`, ReasonInvalid},
		{"voltix horizon too long", VendorVoltix, `{"id":"m-2","position":{"type":"surplus","kwh":1},"validity":{"until":"2026-12-16T10:00:00Z"},"version":1,"zoneCode":"Z1","measuredAt":"2026-10-16T10:00:00Z"}`, ReasonInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := a.Translate(c.vendor, []byte(c.body))
			var re *RejectError
			require.ErrorAs(t, err, &re)
			require.Equal(t, c.reason, re.Reason)
			require.Equal(t, c.vendor, re.Vendor)
		})
	}
}

func TestZoneOfIsDeterministicAndCoversAllZones(t *testing.T) {
	require.Equal(t, "z-2", ZoneOf("m-0001", 4))
	require.Equal(t, "z-1", ZoneOf("m-0004", 4))
	require.Equal(t, "z-1", ZoneOf("m-0001", 1))
	require.Equal(t, ZoneOf("s16-0042", 4), ZoneOf("s16-0042", 4))

	seen := map[string]int{}
	for i := 1; i <= 200; i++ {
		seen[ZoneOf(meter(i), 4)]++
	}
	require.Equal(t, map[string]int{"z-1": 50, "z-2": 50, "z-3": 50, "z-4": 50}, seen)

	// sin dígitos finales: hash estable dentro de rango
	z := ZoneNumber("meter-abc", 4)
	require.Equal(t, z, ZoneNumber("meter-abc", 4))
	require.True(t, z >= 1 && z <= 4)
}

func TestNewRejectsZeroZones(t *testing.T) {
	_, err := New(0)
	require.Error(t, err)
}

func meter(i int) string {
	return "m-" + string(rune('0'+i/1000%10)) + string(rune('0'+i/100%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10))
}
