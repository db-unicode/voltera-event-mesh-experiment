// Package acl es la capa anticorrupción del declaration-gateway: traduce cada formato de
// fabricante de medidor al lenguaje del dominio (events.PositionDeclared). El resto del
// sistema no conoce los formatos de origen; un formato desconocido o inválido se rechaza
// aquí, con motivo, y no entra al dominio.
package acl

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"regexp"
	"strconv"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// Valores del header x-vendor de meter-declarations.
const (
	VendorAcme   = "acme-v1"
	VendorVoltix = "voltix-2"
)

// Motivos de rechazo (etiqueta reason de declarations_rejected_total).
const (
	ReasonUnknownVendor = "unknown_vendor"
	ReasonMalformed     = "malformed"
	ReasonInvalid       = "invalid"
)

// maxHorizon acota validUntil - occurredAt: un medidor no declara a más de un día.
const maxHorizon = 24 * time.Hour

// Declaration es el resultado de traducir un mensaje de medidor.
type Declaration struct {
	Position   events.PositionDeclared
	Version    int64
	OccurredAt time.Time
}

// RejectError es un rechazo de la ACL. Reason es una de las constantes Reason*.
type RejectError struct {
	Vendor string
	Reason string
	Err    error
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("acl: rejected (%s, vendor %q): %v", e.Reason, e.Vendor, e.Err)
}
func (e *RejectError) Unwrap() error { return e.Err }

// translator traduce el cuerpo de un fabricante.
type translator func(body []byte, zones int) (Declaration, error)

// ACL traduce mensajes de medidores. zones es el número de zonas del mercado: acme no
// informa la zona y la ACL la deriva del medidor con ZoneOf.
type ACL struct {
	zones       int
	translators map[string]translator
}

// New crea la ACL con los fabricantes conocidos. zones debe ser >= 1.
func New(zones int) (*ACL, error) {
	if zones < 1 {
		return nil, fmt.Errorf("acl: zones must be >= 1, got %d", zones)
	}
	return &ACL{zones: zones, translators: map[string]translator{
		VendorAcme:   translateAcme,
		VendorVoltix: translateVoltix,
	}}, nil
}

// Vendors devuelve los formatos conocidos.
func (a *ACL) Vendors() []string { return []string{VendorAcme, VendorVoltix} }

// Known indica si vendor es un formato conocido.
func (a *ACL) Known(vendor string) bool { _, ok := a.translators[vendor]; return ok }

// Translate traduce body según vendor (header x-vendor). Siempre devuelve *RejectError
// si no puede: vendor vacío o desconocido, JSON roto o campos inválidos.
func (a *ACL) Translate(vendor string, body []byte) (Declaration, error) {
	tr, ok := a.translators[vendor]
	if !ok {
		return Declaration{}, &RejectError{Vendor: vendor, Reason: ReasonUnknownVendor, Err: errors.New("unknown x-vendor")}
	}
	d, err := tr(body, a.zones)
	if err != nil {
		var re *RejectError
		if errors.As(err, &re) {
			re.Vendor = vendor
			return Declaration{}, re
		}
		return Declaration{}, &RejectError{Vendor: vendor, Reason: ReasonInvalid, Err: err}
	}
	if err := validate(d); err != nil {
		return Declaration{}, &RejectError{Vendor: vendor, Reason: ReasonInvalid, Err: err}
	}
	return d, nil
}

var meterIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// validate aplica las reglas del dominio, comunes a todos los fabricantes.
func validate(d Declaration) error {
	p := d.Position
	switch {
	case !meterIDPattern.MatchString(p.MeterID):
		return fmt.Errorf("meter id %q must match %s", p.MeterID, meterIDPattern)
	case p.Zone == "":
		return errors.New("empty zone")
	case math.IsNaN(p.KWh) || math.IsInf(p.KWh, 0):
		return errors.New("kwh is not finite")
	case d.Version < 1:
		return fmt.Errorf("version must be >= 1, got %d", d.Version)
	case d.OccurredAt.IsZero():
		return errors.New("missing timestamp")
	case !p.ValidUntil.After(d.OccurredAt):
		return errors.New("validUntil is not after the measurement time")
	case p.ValidUntil.Sub(d.OccurredAt) > maxHorizon:
		return fmt.Errorf("validUntil is more than %s after the measurement", maxHorizon)
	}
	return p.Validate()
}

func malformed(err error) error {
	return &RejectError{Reason: ReasonMalformed, Err: err}
}

// ZoneOf asigna la zona "z-<n>" (1..zones) a un medidor. Es la tabla determinista con la
// que el gateway completa los mensajes de acme (que no traen zona) y con la que meter-sim
// decide a qué zona pertenece cada medidor: n = dígitos finales del id mod zones, más 1
// (m-0001 -> z-2 con 4 zonas; m-0004 -> z-1). Sin dígitos finales se usa FNV-1a del id.
func ZoneOf(meterID string, zones int) string {
	return "z-" + strconv.Itoa(ZoneNumber(meterID, zones))
}

// ZoneNumber es el <n> de ZoneOf.
func ZoneNumber(meterID string, zones int) int {
	if zones < 1 {
		zones = 1
	}
	end := len(meterID)
	start := end
	for start > 0 && meterID[start-1] >= '0' && meterID[start-1] <= '9' && end-start < 15 {
		start--
	}
	if start < end {
		if n, err := strconv.ParseUint(meterID[start:end], 10, 64); err == nil {
			return int(n%uint64(zones)) + 1
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(meterID))
	return int(h.Sum32()%uint32(zones)) + 1
}
