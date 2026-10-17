package metersim

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
)

// Curve es el calendario de llegadas: Time(n) es el instante (desde el inicio) en que
// vence la n-ésima declaración (n >= 1). Con Rate == 0 todo vence en 0 (sin límite).
type Curve struct {
	Rate float64       // declaraciones/s en la meseta
	Ramp time.Duration // rampa lineal de 0 a Rate antes de la meseta
}

// Time devuelve cuándo vence la declaración n-ésima.
func (c Curve) Time(n int) time.Duration {
	if c.Rate <= 0 || n < 1 {
		return 0
	}
	ramp := c.Ramp.Seconds()
	inRamp := c.Rate * ramp / 2 // declaraciones que caben en la rampa
	var t float64
	if ramp > 0 && float64(n) <= inRamp {
		t = math.Sqrt(2 * ramp * float64(n) / c.Rate)
	} else {
		t = ramp + (float64(n)-inRamp)/c.Rate
	}
	return time.Duration(t * float64(time.Second))
}

// Count devuelve cuántas declaraciones vencen dentro de d.
func (c Curve) Count(d time.Duration) int {
	if c.Rate <= 0 || d <= 0 {
		return 0
	}
	s, ramp := d.Seconds(), c.Ramp.Seconds()
	if ramp > 0 && s < ramp {
		return int(c.Rate * s * s / (2 * ramp))
	}
	return int(c.Rate*ramp/2 + c.Rate*(s-ramp))
}

// Plan es la secuencia de declaraciones de una corrida.
type Plan interface {
	// Total son las declaraciones a enviar.
	Total() int
	// At devuelve la declaración i (0 <= i < Total). Es pura: mismo i, misma declaración.
	At(i int) Declaration
	// ExpectedPairs son los pares SURPLUS/DEFICIT completos enviados (solo pairs; 0 en el resto).
	ExpectedPairs() int
}

// NewPlan construye el plan y su curva a partir de las opciones ya resueltas.
func NewPlan(o Options) (Plan, Curve, error) {
	switch o.Profile {
	case ProfileSteady, ProfileProbe:
		c := Curve{Rate: o.Rate}
		return &cyclePlan{o: o, total: c.Count(o.Duration)}, c, nil
	case ProfileSpike:
		c := Curve{Rate: o.Rate, Ramp: min(o.Ramp, o.Duration/2)}
		return &cyclePlan{o: o, total: c.Count(o.Duration)}, c, nil
	case ProfilePairs:
		c := Curve{Rate: o.Rate}
		p, err := newPairsPlan(o, c.Count(o.Duration))
		return p, c, err
	case ProfileOnce, ProfileDirectPositions:
		return &oncePlan{o: o}, Curve{Rate: o.Rate}, nil
	case ProfileDup:
		return newDupPlan(o), Curve{Rate: o.Rate}, nil
	}
	return nil, Curve{}, fmt.Errorf("metersim: unknown profile %q", o.Profile)
}

// cyclePlan recorre los medidores en orden; en cada vuelta sube la versión (steady y spike).
type cyclePlan struct {
	o     Options
	total int
}

func (p *cyclePlan) Total() int         { return p.total }
func (p *cyclePlan) ExpectedPairs() int { return 0 }
func (p *cyclePlan) At(i int) Declaration {
	return p.o.declare(i%p.o.Meters+1, p.o.VersionBase+int64(i/p.o.Meters))
}

// oncePlan: cada medidor declara una sola vez.
type oncePlan struct{ o Options }

func (p *oncePlan) Total() int           { return p.o.Meters }
func (p *oncePlan) ExpectedPairs() int   { return 0 }
func (p *oncePlan) At(i int) Declaration { return p.o.declare(i+1, p.o.VersionBase) }

// dupPlan: el lote de once repetido DupK veces en orden mezclado (determinista por semilla).
type dupPlan struct {
	o    Options
	perm []int
}

func newDupPlan(o Options) *dupPlan {
	total := o.Meters * o.DupK
	perm := make([]int, total)
	for i := range perm {
		perm[i] = i % o.Meters
	}
	rng := rand.New(rand.NewPCG(uint64(o.Seed), 0x5851f42d4c957f2d))
	rng.Shuffle(total, func(a, b int) { perm[a], perm[b] = perm[b], perm[a] })
	return &dupPlan{o: o, perm: perm}
}

func (p *dupPlan) Total() int         { return len(p.perm) }
func (p *dupPlan) ExpectedPairs() int { return 0 }
func (p *dupPlan) At(i int) Declaration {
	return p.o.declare(p.perm[i]+1, p.o.VersionBase)
}

// pairsPlan: en cada zona los medidores se emparejan de dos en dos (SURPLUS, DEFICIT) con la
// misma energía; el par se declara seguido y la vuelta siguiente sube la versión.
type pairsPlan struct {
	o     Options
	slots []pairSlot
	total int
}

type pairSlot struct {
	meterIdx int
	zone     int
	pair     int // índice del par dentro de la zona
	side     events.Side
}

func newPairsPlan(o Options, wanted int) (*pairsPlan, error) {
	byZone := map[int][]int{}
	for n := 1; n <= o.Meters; n++ {
		z := acl.ZoneNumber(meterID(o.Prefix, n), o.Zones)
		byZone[z] = append(byZone[z], n)
	}
	zones := make([]int, 0, len(byZone))
	for z := range byZone {
		zones = append(zones, z)
	}
	sort.Ints(zones)
	maxPairs := 0
	for _, z := range zones {
		maxPairs = max(maxPairs, len(byZone[z])/2)
	}
	// Orden: par 0 de todas las zonas, luego par 1... (reparte la carga entre zonas).
	var slots []pairSlot
	for k := range maxPairs {
		for _, z := range zones {
			ms := byZone[z]
			if 2*k+1 < len(ms) {
				slots = append(slots,
					pairSlot{meterIdx: ms[2*k], zone: z, pair: k, side: events.SideSurplus},
					pairSlot{meterIdx: ms[2*k+1], zone: z, pair: k, side: events.SideDeficit})
			}
		}
	}
	if len(slots) == 0 {
		return nil, errors.New("metersim: pairs needs at least two meters in one zone (raise --meters or lower --zones)")
	}
	// Total par: nunca queda un SURPLUS sin su DEFICIT al final.
	return &pairsPlan{o: o, slots: slots, total: wanted - wanted%2}, nil
}

func (p *pairsPlan) Total() int { return p.total }
func (p *pairsPlan) At(i int) Declaration {
	s := p.slots[i%len(p.slots)]
	round := i / len(p.slots)
	version := p.o.VersionBase + int64(round)
	kwh := kwhFrom(mix(uint64(p.o.Seed), uint64(s.zone), uint64(s.pair), uint64(round)))
	return p.o.build(s.meterIdx, meterID(p.o.Prefix, s.meterIdx), version, s.side, kwh)
}
func (p *pairsPlan) ExpectedPairs() int { return p.total / 2 }
