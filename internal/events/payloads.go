package events

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Side es el lado de una posición en el mercado.
type Side string

const (
	SideSurplus Side = "SURPLUS"
	SideDeficit Side = "DEFICIT"
)

// Valid indica si s es SURPLUS o DEFICIT.
func (s Side) Valid() bool { return s == SideSurplus || s == SideDeficit }

// PositionDeclared es el estado completo de la posición de un medidor.
type PositionDeclared struct {
	MeterID    string    `json:"meterId"`
	Side       Side      `json:"side"`
	KWh        float64   `json:"kwh"`
	ValidUntil time.Time `json:"validUntil"`
	Zone       string    `json:"zone"`
}

// Validate comprueba los campos del payload.
func (p PositionDeclared) Validate() error {
	switch {
	case p.MeterID == "":
		return fmt.Errorf("events: PositionDeclared without meterId")
	case !p.Side.Valid():
		return fmt.Errorf("events: PositionDeclared invalid side %q", p.Side)
	case p.KWh < 0:
		return fmt.Errorf("events: PositionDeclared negative kwh %v", p.KWh)
	}
	return nil
}

// PriceUpdated es el precio vigente de una región.
type PriceUpdated struct {
	Region      string    `json:"region"`
	PricePerKWh float64   `json:"pricePerKwh"`
	ValidUntil  time.Time `json:"validUntil"`
}

// EnergyMatched es un emparejamiento oferente-demandante dentro de una ventana.
type EnergyMatched struct {
	MatchID       string  `json:"matchId"`
	SellerMeterID string  `json:"sellerMeterId"`
	BuyerMeterID  string  `json:"buyerMeterId"`
	KWh           float64 `json:"kwh"`
	PricePerKWh   float64 `json:"pricePerKwh"`
	Zone          string  `json:"zone"`
	// Window identifica la ventana de matching (p. ej. inicio en RFC3339).
	Window string `json:"window"`
}

// MatchID calcula el id determinista de un match: reprocesar la misma ventana con
// el mismo par produce el mismo id, y un índice único evita matches dobles.
// Formato: "match-" + 32 hex (sha256 truncado de window, seller, buyer).
func MatchID(window, seller, buyer string) string {
	h := sha256.New()
	for _, s := range []string{window, seller, buyer} {
		// Prefijo de longitud: ("ab","c") y ("a","bc") no colisionan.
		_, _ = fmt.Fprintf(h, "%d:%s;", len(s), s)
	}
	return "match-" + hex.EncodeToString(h.Sum(nil)[:16])
}
