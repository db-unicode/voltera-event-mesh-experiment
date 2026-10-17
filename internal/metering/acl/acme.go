package acl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// acmeV1 es el formato de acme-v1:
//
//	{"meter":"m-0001","mode":"SELL|BUY","energyWh":4200,"horizonSec":120,"seq":17,"ts":"RFC3339Nano"}
//
// Energía en Wh, vigencia relativa (horizonSec desde ts), sin zona.
type acmeV1 struct {
	Meter      *string  `json:"meter"`
	Mode       *string  `json:"mode"`
	EnergyWh   *float64 `json:"energyWh"`
	HorizonSec *float64 `json:"horizonSec"`
	Seq        *int64   `json:"seq"`
	TS         *string  `json:"ts"`
}

func translateAcme(body []byte, zones int) (Declaration, error) {
	var m acmeV1
	if err := decode(body, &m); err != nil {
		return Declaration{}, malformed(err)
	}
	if m.Meter == nil || m.Mode == nil || m.EnergyWh == nil || m.HorizonSec == nil || m.Seq == nil || m.TS == nil {
		return Declaration{}, errors.New("acme-v1 requires meter, mode, energyWh, horizonSec, seq and ts")
	}
	var side events.Side
	switch *m.Mode {
	case "SELL":
		side = events.SideSurplus
	case "BUY":
		side = events.SideDeficit
	default:
		return Declaration{}, fmt.Errorf("acme-v1 mode %q must be SELL or BUY", *m.Mode)
	}
	if *m.EnergyWh < 0 || math.IsNaN(*m.EnergyWh) {
		return Declaration{}, fmt.Errorf("acme-v1 energyWh %v must be >= 0", *m.EnergyWh)
	}
	if *m.HorizonSec <= 0 || *m.HorizonSec > maxHorizon.Seconds() {
		return Declaration{}, fmt.Errorf("acme-v1 horizonSec %v out of range", *m.HorizonSec)
	}
	ts, err := time.Parse(time.RFC3339Nano, *m.TS)
	if err != nil {
		return Declaration{}, fmt.Errorf("acme-v1 ts: %w", err)
	}
	ts = ts.UTC()
	return Declaration{
		Position: events.PositionDeclared{
			MeterID:    *m.Meter,
			Side:       side,
			KWh:        *m.EnergyWh / 1000,
			ValidUntil: ts.Add(time.Duration(*m.HorizonSec * float64(time.Second))),
			Zone:       ZoneOf(*m.Meter, zones),
		},
		Version:    *m.Seq,
		OccurredAt: ts,
	}, nil
}

// decode parsea JSON rechazando basura detrás del objeto.
func decode(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after JSON object")
	}
	return nil
}
