package matching

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

// Engine ejecuta una ventana de matching cada Window.
type Engine struct {
	Region  string
	Window  time.Duration
	Source  Source
	Store   *Store
	Rules   *RulesLoader
	Metrics *telemetry.DomainMetrics
	Log     *slog.Logger
	// OnCommit se invoca tras confirmar matches (despierta al relay de la outbox).
	OnCommit func()
	Now      func() time.Time

	mu   sync.Mutex
	last WindowStats
}

// WindowStats resume la última ventana (sección "lastWindow" de /debug/state).
type WindowStats struct {
	Window     string    `json:"window"`
	At         time.Time `json:"at"`
	Zones      int       `json:"zones"`
	Considered int       `json:"considered"`
	Eligible   int       `json:"eligible"`
	Created    int       `json:"created"`
	Discarded  int       `json:"discarded"`
	Invalid    int       `json:"invalidKeys"`
	RuleErrors int       `json:"ruleErrors"`
	NoPrice    bool      `json:"noPrice,omitempty"`
	DurationMS float64   `json:"durationMs"`
	Rules      string    `json:"rules"`
}

// LastWindow devuelve las estadísticas de la última ventana.
func (e *Engine) LastWindow() WindowStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last
}

// Run lanza una ventana por cada tick hasta que ctx se cancele (devuelve nil).
func (e *Engine) Run(ctx context.Context) error {
	if e.Now == nil {
		e.Now = time.Now
	}
	t := time.NewTicker(e.Window)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := e.RunWindow(ctx); err != nil && ctx.Err() == nil {
				e.Log.Error("matching window failed", "stage", "apply", "result", "error", "error", err)
			}
		}
	}
}

// RunWindow procesa la ventana actual: lee la vista, empareja por zona y persiste.
func (e *Engine) RunWindow(ctx context.Context) error {
	if e.Now == nil {
		e.Now = time.Now
	}
	start := e.Now()
	windowID := start.UTC().Truncate(e.Window).Format(time.RFC3339)
	stats := WindowStats{Window: windowID, At: start.UTC()}
	defer func() {
		stats.DurationMS = float64(e.Now().Sub(start).Microseconds()) / 1000
		e.mu.Lock()
		e.last = stats
		e.mu.Unlock()
		e.Metrics.MatchWindowSeconds.Observe(e.Now().Sub(start).Seconds())
	}()

	rules := e.Rules.Current()
	stats.Rules = rules.Version

	price, err := e.Source.Price(ctx, e.Region)
	switch {
	case errors.Is(err, view.ErrNotFound):
		stats.NoPrice = true
		return nil // sin precio vigente no hay matching
	case err != nil:
		return err
	case !price.ValidUntil.After(start):
		stats.NoPrice = true
		return nil
	}

	byZone, invalid, err := e.Source.Positions(ctx)
	if err != nil {
		return err
	}
	stats.Invalid = invalid
	var all []Position
	for _, ps := range byZone {
		all = append(all, ps...)
	}
	matched, err := e.Store.MatchedRefs(ctx, all)
	if err != nil {
		return err
	}
	open := make(map[string][]Position, len(byZone))
	for z, ps := range byZone {
		for _, p := range ps {
			if _, done := matched[p.Ref()]; !done {
				open[z] = append(open[z], p)
			}
		}
	}
	byZone = open
	zones := make([]string, 0, len(byZone))
	for z := range byZone {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	stats.Zones = len(zones)

	var firstErr error
	for _, zone := range zones {
		plan := PlanZone(byZone[zone], price, rules, start, e.Log)
		stats.Considered += plan.Considered
		stats.Eligible += plan.Eligible
		stats.RuleErrors += plan.RuleErrors
		if len(plan.Pairs) == 0 {
			continue
		}
		created, discarded, err := e.Store.Persist(ctx, windowID, zone, price, plan.Pairs)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		stats.Created += len(created)
		stats.Discarded += discarded
		for _, c := range created {
			e.Metrics.MatchesCreated.WithLabelValues(e.Region).Inc()
			telemetry.LogStage(e.Log, telemetry.StageCommit, c.Event, telemetry.ResultOK,
				"matchId", c.Payload.MatchID, "seller", c.Payload.SellerMeterID, "buyer", c.Payload.BuyerMeterID,
				"kwh", c.Payload.KWh, "zone", zone, "window", windowID, "rules", rules.Version)
		}
		if discarded > 0 {
			e.Log.Info("pairs discarded: position already matched", "stage", "apply", "result", telemetry.ResultDuplicate,
				"zone", zone, "window", windowID, "discarded", discarded)
		}
		if len(created) > 0 && e.OnCommit != nil {
			e.OnCommit()
		}
	}
	return firstErr
}
