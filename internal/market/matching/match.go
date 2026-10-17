package matching

import (
	"log/slog"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// Plan es el resultado de emparejar una zona.
type Plan struct {
	Pairs       []Pair
	Considered  int // posiciones SURPLUS/DEFICIT vigentes
	Eligible    int // las que pasaron la regla eligible
	RuleErrors  int // evaluaciones de regla que fallaron (se tratan como "no")
	Unmatched   int // elegibles sin pareja
	SurplusLeft int
	DeficitLeft int
}

// PlanZone empareja las posiciones vigentes de una zona. Es determinista: oferentes y
// demandantes se ordenan por kwh descendente y meterId ascendente, y cada oferente se
// une al primer demandante libre con el que la regla pair se cumple. kwh = min de ambos.
// Una posición entra en un solo par.
func PlanZone(positions []Position, price Price, rules *Rules, now time.Time, log *slog.Logger) Plan {
	var plan Plan
	var sellers, buyers []Position
	for _, p := range positions {
		if !p.Side.Valid() || !p.ValidUntil.After(now) || p.KWh <= 0 {
			continue
		}
		plan.Considered++
		ok, err := rules.Eligible(p, price)
		if err != nil {
			plan.RuleErrors++
			log.Warn("eligible rule failed", "stage", "apply", "result", "error", "meterId", p.MeterID, "rules", rules.Version, "error", err)
			continue
		}
		if !ok {
			continue
		}
		plan.Eligible++
		if p.Side == events.SideSurplus {
			sellers = append(sellers, p)
		} else {
			buyers = append(buyers, p)
		}
	}
	sortPositions(sellers)
	sortPositions(buyers)

	used := make([]bool, len(buyers))
	for _, s := range sellers {
		for i, b := range buyers {
			if used[i] {
				continue
			}
			ok, err := rules.CanPair(s, b, price)
			if err != nil {
				plan.RuleErrors++
				log.Warn("pair rule failed", "stage", "apply", "result", "error", "seller", s.MeterID, "buyer", b.MeterID, "rules", rules.Version, "error", err)
				continue
			}
			if !ok {
				continue
			}
			used[i] = true
			plan.Pairs = append(plan.Pairs, Pair{Seller: s, Buyer: b, KWh: min(s.KWh, b.KWh)})
			break
		}
	}
	plan.SurplusLeft = len(sellers) - len(plan.Pairs)
	plan.DeficitLeft = len(buyers) - len(plan.Pairs)
	plan.Unmatched = plan.SurplusLeft + plan.DeficitLeft
	return plan
}
