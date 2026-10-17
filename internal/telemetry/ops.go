package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/buildinfo"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
)

// Info identifica al servicio en métricas y /debug/state.
type Info struct {
	Service  string `json:"service"`
	Version  string `json:"version"`
	Region   string `json:"region"`
	Instance string `json:"instance"`
}

// InfoFromEnv arma Info con REGION, POD_NAME (o hostname) y buildinfo.Version.
func InfoFromEnv(service string) Info {
	inst := os.Getenv("POD_NAME")
	if inst == "" {
		inst, _ = os.Hostname()
	}
	return Info{Service: service, Version: buildinfo.Version, Region: os.Getenv("REGION"), Instance: inst}
}

// Service agrupa el registro Prometheus, el estado de /debug/state y el servidor de
// operación (/healthz, /readyz, /metrics, /debug/state, /admin/failpoint).
type Service struct {
	Info Info
	// Registry es el registro crudo (para promhttp); Reg registra con la etiqueta service.
	Registry *prometheus.Registry
	Reg      prometheus.Registerer
	Metrics  *Metrics
	Logger   *slog.Logger

	// Failpoints, si no es nil, se muestra en /debug/state y (con EXPERIMENTS=1) se
	// controla con POST /admin/failpoint.
	Failpoints *failpoint.Registry
	// Experiments habilita /admin/failpoint; por defecto failpoint.Experiments() (env EXPERIMENTS=1).
	Experiments bool

	startedAt time.Time

	mu         sync.Mutex
	ready      map[string]func(context.Context) error
	state      map[string]func(context.Context) any
	lastEvent  string
	lastAt     time.Time
	consumed   map[string]int64
	notReadyBy bool
}

// New crea el servicio de telemetría con las métricas comunes y los collectors de
// runtime y proceso de client_golang. Todas las métricas llevan service=<nombre>.
func New(info Info, logger *slog.Logger) *Service {
	raw := prometheus.NewRegistry()
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"service": info.Service}, raw)
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		Info:        info,
		Registry:    raw,
		Reg:         reg,
		Metrics:     NewMetrics(reg),
		Logger:      logger,
		Experiments: failpoint.Experiments(),
		startedAt:   time.Now().UTC(),
		ready:       map[string]func(context.Context) error{},
		state:       map[string]func(context.Context) any{},
		consumed:    map[string]int64{ResultOK: 0, ResultDuplicate: 0, ResultStale: 0, ResultError: 0},
	}
}

// AddReadyCheck registra un check de /readyz (se ejecutan todos, con timeout de 2 s).
func (s *Service) AddReadyCheck(name string, fn func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready[name] = fn
}

// AddState añade una sección a /debug/state (p. ej. "outbox").
func (s *Service) AddState(key string, fn func(context.Context) any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key] = fn
}

// SetNotReady fuerza /readyz a 503 (p. ej. al iniciar el apagado ordenado).
func (s *Service) SetNotReady(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notReadyBy = v
}

// Published cuenta un evento publicado en un canal.
func (s *Service) Published(env events.Envelope, channel string) {
	s.Metrics.EventsPublished.WithLabelValues(env.Type, channel).Inc()
}

// Consumed cuenta un evento consumido con su resultado (ok|duplicate|stale|error),
// actualiza los contadores de duplicados/viejos y el estado de /debug/state.
func (s *Service) Consumed(env events.Envelope, channel, result string) {
	s.Metrics.EventsConsumed.WithLabelValues(env.Type, channel, result).Inc()
	switch result {
	case ResultDuplicate:
		s.Metrics.DuplicatesDropped.WithLabelValues(env.Type).Inc()
	case ResultStale:
		s.Metrics.StaleDropped.WithLabelValues(env.Type).Inc()
	}
	s.mu.Lock()
	s.consumed[result]++
	s.lastEvent, s.lastAt = env.EventID, time.Now().UTC()
	s.mu.Unlock()
}

// Handler devuelve el mux de operación.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /debug/state", s.debugState)
	if s.Experiments && s.Failpoints != nil {
		mux.Handle("/admin/failpoint", s.Failpoints.Handler(true))
	}
	return mux
}

func (s *Service) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	s.mu.Lock()
	checks := make(map[string]func(context.Context) error, len(s.ready))
	for k, v := range s.ready {
		checks[k] = v
	}
	forced := s.notReadyBy
	s.mu.Unlock()

	failures := map[string]string{}
	if forced {
		failures["draining"] = "shutting down"
	}
	for name, fn := range checks {
		if err := fn(ctx); err != nil {
			failures[name] = err.Error()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if len(failures) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": false, "failures": failures})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"ready": true})
}

func (s *Service) debugState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	s.mu.Lock()
	out := map[string]any{
		"service":     s.Info.Service,
		"version":     s.Info.Version,
		"region":      s.Info.Region,
		"instance":    s.Info.Instance,
		"startedAt":   s.startedAt,
		"lastEventId": s.lastEvent,
		"consumed":    cloneCounts(s.consumed),
	}
	if !s.lastAt.IsZero() {
		out["lastEventAt"] = s.lastAt
	}
	providers := make(map[string]func(context.Context) any, len(s.state))
	for k, v := range s.state {
		providers[k] = v
	}
	s.mu.Unlock()

	keys := make([]string, 0, len(providers))
	for k := range providers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out[k] = providers[k](ctx)
	}
	if s.Failpoints != nil {
		out["failpoints"] = s.Failpoints.Snapshot()
	} else {
		out["failpoints"] = map[string]string{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func cloneCounts(m map[string]int64) map[string]int64 {
	c := make(map[string]int64, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// Serve atiende el servidor de operación en addr hasta que ctx se cancele, y entonces
// lo apaga con gracia (5 s). Devuelve nil en un apagado normal.
func (s *Service) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	s.Logger.Info("ops server listening", "stage", "start", "addr", addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
