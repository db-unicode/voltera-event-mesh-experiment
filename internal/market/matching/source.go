package matching

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

// Prefijos y keys de la vista (contrato con market-projector).
const (
	positionPrefix = "pos:"
	pricePrefix    = "price:"
)

// Source entrega el estado de mercado de una ventana.
type Source interface {
	// Positions devuelve las posiciones de la vista agrupadas por zona, y cuántas keys
	// no se pudieron interpretar.
	Positions(ctx context.Context) (byZone map[string][]Position, invalid int, err error)
	// Price devuelve el precio de la región; view.ErrNotFound si no hay.
	Price(ctx context.Context, region string) (Price, error)
}

// ViewSource lee la vista Redis con view.Store. Es solo lectura: este servicio nunca
// escribe en la vista.
type ViewSource struct {
	Store *view.Store
	// KeyPrefix se antepone a todas las keys (vacío en producción; aísla los tests).
	KeyPrefix string
}

var _ Source = ViewSource{}

// Positions barre pos:* y agrupa por el campo zone.
func (v ViewSource) Positions(ctx context.Context) (map[string][]Position, int, error) {
	entries, err := v.Store.ScanByPrefix(ctx, v.KeyPrefix+positionPrefix)
	if err != nil {
		return nil, 0, err
	}
	out := map[string][]Position{}
	invalid := 0
	for _, e := range entries {
		p, err := parsePosition(e)
		if err != nil {
			invalid++
			continue
		}
		out[p.Zone] = append(out[p.Zone], p)
	}
	return out, invalid, nil
}

// Price lee price:<region>.
func (v ViewSource) Price(ctx context.Context, region string) (Price, error) {
	e, err := v.Store.Get(ctx, v.KeyPrefix+pricePrefix+region)
	if err != nil {
		return Price{}, err
	}
	pr, err := strconv.ParseFloat(e.Fields["pricePerKwh"], 64)
	if err != nil {
		return Price{}, fmt.Errorf("price: invalid pricePerKwh %q", e.Fields["pricePerKwh"])
	}
	until, err := time.Parse(time.RFC3339Nano, e.Fields["validUntil"])
	if err != nil {
		return Price{}, fmt.Errorf("price: invalid validUntil %q", e.Fields["validUntil"])
	}
	return Price{PricePerKWh: pr, ValidUntil: until.UTC(), Version: e.Version}, nil
}

func parsePosition(e view.Entry) (Position, error) {
	f := e.Fields
	kwh, err := strconv.ParseFloat(f["kwh"], 64)
	if err != nil {
		return Position{}, fmt.Errorf("%s: invalid kwh %q", e.Key, f["kwh"])
	}
	until, err := time.Parse(time.RFC3339Nano, f["validUntil"])
	if err != nil {
		return Position{}, fmt.Errorf("%s: invalid validUntil %q", e.Key, f["validUntil"])
	}
	side := events.Side(f["side"])
	if f["meterId"] == "" || f["zone"] == "" || !side.Valid() {
		return Position{}, errors.New(e.Key + ": missing meterId, zone or side")
	}
	return Position{MeterID: f["meterId"], Side: side, KWh: kwh, ValidUntil: until.UTC(), Zone: f["zone"], Version: e.Version}, nil
}
