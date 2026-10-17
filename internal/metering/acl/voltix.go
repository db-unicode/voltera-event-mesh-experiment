package acl

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// voltix2 es el formato de voltix-2:
//
//	{"id":"m-0002","position":{"type":"surplus|deficit","kwh":3.1},"validity":{"until":"RFC3339Nano"},
//	 "version":9,"zoneCode":"Z3","measuredAt":"RFC3339Nano"}
//
// Energía en kWh, vigencia absoluta, zona explícita ("Z<n>").
type voltix2 struct {
	ID       *string `json:"id"`
	Position *struct {
		Type *string  `json:"type"`
		KWh  *float64 `json:"kwh"`
	} `json:"position"`
	Validity *struct {
		Until *string `json:"until"`
	} `json:"validity"`
	Version    *int64  `json:"version"`
	ZoneCode   *string `json:"zoneCode"`
	MeasuredAt *string `json:"measuredAt"`
}

var zoneCodePattern = regexp.MustCompile(`^Z([1-9][0-9]{0,3})$`)

func translateVoltix(body []byte, _ int) (Declaration, error) {
	var m voltix2
	if err := decode(body, &m); err != nil {
		return Declaration{}, malformed(err)
	}
	if m.ID == nil || m.Position == nil || m.Position.Type == nil || m.Position.KWh == nil ||
		m.Validity == nil || m.Validity.Until == nil || m.Version == nil || m.ZoneCode == nil || m.MeasuredAt == nil {
		return Declaration{}, errors.New("voltix-2 requires id, position.type, position.kwh, validity.until, version, zoneCode and measuredAt")
	}
	var side events.Side
	switch strings.ToLower(*m.Position.Type) {
	case "surplus":
		side = events.SideSurplus
	case "deficit":
		side = events.SideDeficit
	default:
		return Declaration{}, fmt.Errorf("voltix-2 position.type %q must be surplus or deficit", *m.Position.Type)
	}
	zm := zoneCodePattern.FindStringSubmatch(*m.ZoneCode)
	if zm == nil {
		return Declaration{}, fmt.Errorf("voltix-2 zoneCode %q must look like Z<n>", *m.ZoneCode)
	}
	until, err := time.Parse(time.RFC3339Nano, *m.Validity.Until)
	if err != nil {
		return Declaration{}, fmt.Errorf("voltix-2 validity.until: %w", err)
	}
	at, err := time.Parse(time.RFC3339Nano, *m.MeasuredAt)
	if err != nil {
		return Declaration{}, fmt.Errorf("voltix-2 measuredAt: %w", err)
	}
	n, _ := strconv.Atoi(zm[1])
	return Declaration{
		Position: events.PositionDeclared{
			MeterID:    *m.ID,
			Side:       side,
			KWh:        *m.Position.KWh,
			ValidUntil: until.UTC(),
			Zone:       "z-" + strconv.Itoa(n),
		},
		Version:    *m.Version,
		OccurredAt: at.UTC(),
	}, nil
}
