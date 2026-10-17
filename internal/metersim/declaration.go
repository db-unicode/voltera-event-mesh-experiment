package metersim

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
)

// eventNamespace deriva los eventId de las declaraciones (UUIDv5 de seed/medidor/versión):
// con la misma semilla, once y dup producen exactamente los mismos ids.
var eventNamespace = uuid.MustParse("6f1c5a52-5d0e-4c1b-9d57-3c0f2a6b7e11")

// Declaration es una declaración por enviar. El instante (ts / measuredAt) se fija al enviarla.
type Declaration struct {
	MeterIdx int // 1..Meters
	MeterID  string
	// Vendor es el valor del header x-vendor; Format, la codificación del cuerpo (acme o voltix).
	Vendor  string
	Format  string
	Side    events.Side
	KWh     float64
	Version int64
	EventID string
	Zone    int // número de zona (1..Zones)
	Horizon time.Duration
}

// meterID arma el id del medidor n: <prefijo>0001.
func meterID(prefix string, n int) string { return fmt.Sprintf("%s%04d", prefix, n) }

// mix es splitmix64 sobre la combinación de las claves: aleatoriedad reproducible sin estado.
func mix(vals ...uint64) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	for _, v := range vals {
		h += v + 0x9e3779b97f4a7c15
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 27)) * 0x94d049bb133111eb
		h ^= h >> 31
	}
	return h
}

// kwhFrom da un kWh en [0,5; 10,0] con 2 decimales a partir de un hash.
func kwhFrom(h uint64) float64 { return math.Round(float64(500+h%9501)/10) / 100 }

// vendorFor elige el formato del medidor n. Con VendorMixed, la mitad de los medidores de
// cada zona usa cada fabricante ((n / zones) par => acme). Un valor distinto de los
// conocidos se envía como header tal cual con cuerpo acme (para probar el rechazo).
func (o Options) vendorFor(n int) (header, format string) {
	switch o.Vendor {
	case VendorMixed:
		if (n/o.Zones)%2 == 0 {
			return acl.VendorAcme, acl.VendorAcme
		}
		return acl.VendorVoltix, acl.VendorVoltix
	case acl.VendorVoltix:
		return acl.VendorVoltix, acl.VendorVoltix
	default:
		return o.Vendor, acl.VendorAcme
	}
}

// declare arma la declaración (medidor n, versión v) con lado y energía derivados de la semilla.
func (o Options) declare(n int, version int64) Declaration {
	id := meterID(o.Prefix, n)
	h := mix(uint64(o.Seed), uint64(n), uint64(version))
	side := events.SideSurplus
	if h&1 == 1 {
		side = events.SideDeficit
	}
	return o.build(n, id, version, side, kwhFrom(h>>1))
}

func (o Options) build(n int, id string, version int64, side events.Side, kwh float64) Declaration {
	header, format := o.vendorFor(n)
	return Declaration{
		MeterIdx: n, MeterID: id, Vendor: header, Format: format, Side: side, KWh: kwh, Version: version,
		EventID: uuid.NewSHA1(eventNamespace, []byte(strconv.FormatInt(o.Seed, 10)+"/"+id+"/"+strconv.FormatInt(version, 10))).String(),
		Zone:    acl.ZoneNumber(id, o.Zones),
		Horizon: o.Horizon,
	}
}

type acmeBody struct {
	Meter      string `json:"meter"`
	Mode       string `json:"mode"`
	EnergyWh   int64  `json:"energyWh"`
	HorizonSec int64  `json:"horizonSec"`
	Seq        int64  `json:"seq"`
	TS         string `json:"ts"`
}

type voltixBody struct {
	ID       string `json:"id"`
	Position struct {
		Type string  `json:"type"`
		KWh  float64 `json:"kwh"`
	} `json:"position"`
	Validity struct {
		Until string `json:"until"`
	} `json:"validity"`
	Version    int64  `json:"version"`
	ZoneCode   string `json:"zoneCode"`
	MeasuredAt string `json:"measuredAt"`
}

// Encode serializa la declaración en el formato de su fabricante con ts como instante de medición.
func (d Declaration) Encode(ts time.Time) ([]byte, error) {
	ts = ts.UTC()
	switch d.Format {
	case acl.VendorAcme:
		mode := "SELL"
		if d.Side == events.SideDeficit {
			mode = "BUY"
		}
		return json.Marshal(acmeBody{Meter: d.MeterID, Mode: mode, EnergyWh: int64(math.Round(d.KWh * 1000)),
			HorizonSec: int64(d.Horizon / time.Second), Seq: d.Version, TS: ts.Format(time.RFC3339Nano)})
	case acl.VendorVoltix:
		var b voltixBody
		b.ID = d.MeterID
		b.Position.Type = "surplus"
		if d.Side == events.SideDeficit {
			b.Position.Type = "deficit"
		}
		b.Position.KWh = d.KWh
		b.Validity.Until = ts.Add(d.Horizon).Format(time.RFC3339Nano)
		b.Version = d.Version
		b.ZoneCode = "Z" + strconv.Itoa(d.Zone)
		b.MeasuredAt = ts.Format(time.RFC3339Nano)
		return json.Marshal(b)
	}
	return nil, fmt.Errorf("metersim: unknown body format %q", d.Format)
}
