// Package matching es el motor de emparejamiento: cada ventana lee las posiciones
// vigentes de la vista Redis (solo lectura), aplica las reglas expr y registra los
// matches en matches-db con su evento EnergyMatched en la outbox (misma transacción).
package matching

import (
	"cmp"
	"slices"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// Position es una posición vigente leída de la vista. Las etiquetas expr son los
// nombres que ven las reglas.
type Position struct {
	MeterID    string      `expr:"meterId"`
	Side       events.Side `expr:"side"`
	KWh        float64     `expr:"kwh"`
	ValidUntil time.Time   `expr:"validUntil"`
	Zone       string      `expr:"zone"`
	Version    int64       `expr:"version"`
}

// Ref identifica la versión exacta de la posición que se empareja.
func (p Position) Ref() string { return p.MeterID + "@" + itoa(p.Version) }

// Price es el precio vigente de la región.
type Price struct {
	PricePerKWh float64   `expr:"pricePerKwh"`
	ValidUntil  time.Time `expr:"validUntil"`
	Version     int64     `expr:"version"`
}

// Pair es un emparejamiento candidato.
type Pair struct {
	Seller Position
	Buyer  Position
	KWh    float64
}

// sortPositions ordena por kwh descendente y meterId ascendente (determinista).
func sortPositions(ps []Position) {
	slices.SortFunc(ps, func(a, b Position) int {
		if c := cmp.Compare(b.KWh, a.KWh); c != 0 {
			return c
		}
		return cmp.Compare(a.MeterID, b.MeterID)
	})
}
