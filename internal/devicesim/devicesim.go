// Package devicesim simula el dispositivo físico que ejecuta las transferencias: acepta
// comandos firmados con HMAC-SHA256, cuenta intentos y efectos únicos por matchId, y
// expone el contador para que los experimentos verifiquen que el efecto ocurrió una vez.
// El estado es en memoria (1 réplica).
package devicesim

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/boot"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/devicecmd"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

const serviceName = "device-sim"

// maxBody acota el cuerpo de un comando.
const maxBody = 64 << 10

// Config se lee de variables de entorno y se valida al arrancar.
type Config struct {
	Region      string
	HTTPAddr    string
	OpsAddr     string
	CommandKey  []byte
	Experiments bool
}

// LoadConfig lee REGION, HTTP_ADDR (:8080), OPS_ADDR (:9090), DEVICE_COMMAND_KEY y EXPERIMENTS.
func LoadConfig(getenv func(string) string) (Config, error) {
	e := boot.NewEnv(getenv)
	c := Config{
		Region:      e.Region(),
		HTTPAddr:    e.Addr("HTTP_ADDR", ":8080"),
		OpsAddr:     e.Addr("OPS_ADDR", ":9090"),
		CommandKey:  []byte(e.Required("DEVICE_COMMAND_KEY")),
		Experiments: getenv("EXPERIMENTS") == "1",
	}
	return c, e.Err()
}

// State es la respuesta de GET /state.
type State struct {
	// Total son los comandos con firma válida recibidos (incluye repetidos).
	Total int `json:"total"`
	// Distinct son los matchId con efecto aplicado.
	Distinct int `json:"distinct"`
	// Duplicates = Total - Distinct: comandos repetidos que no produjeron efecto.
	Duplicates int `json:"duplicates"`
	// Rejected son los comandos con firma inválida (401).
	Rejected  int            `json:"rejected"`
	ByMatchID map[string]int `json:"byMatchId"`
}

// Server es el dispositivo simulado.
type Server struct {
	key         []byte
	experiments bool
	log         *slog.Logger
	commands    *prometheus.CounterVec

	mu       sync.Mutex
	byMatch  map[string]int
	total    int
	rejected int
}

// NewServer crea el dispositivo. reg puede ser nil.
func NewServer(key []byte, experiments bool, log *slog.Logger, reg prometheus.Registerer) *Server {
	s := &Server{key: key, experiments: experiments, log: log, byMatch: map[string]int{},
		commands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "device_commands_total", Help: "Comandos recibidos por resultado."}, []string{"result"})}
	for _, r := range []string{"applied", "duplicate", "rejected"} {
		s.commands.WithLabelValues(r)
	}
	if reg != nil {
		reg.MustRegister(s.commands)
	}
	return s
}

// Handler devuelve el mux de la API (puerto http-api).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /commands", s.postCommand)
	mux.HandleFunc("GET /state", s.getState)
	mux.HandleFunc("POST /admin/reset", s.reset)
	return mux
}

func (s *Server) postCommand(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	if !devicecmd.Verify(s.key, body, r.Header.Get(devicecmd.SignatureHeader)) {
		s.mu.Lock()
		s.rejected++
		s.mu.Unlock()
		s.commands.WithLabelValues("rejected").Inc()
		s.log.Warn("command rejected: bad signature", "stage", "command", "result", "error", "reason", "bad_signature")
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	var cmd devicecmd.Command
	if err := json.Unmarshal(body, &cmd); err != nil || cmd.Validate() != nil {
		http.Error(w, "invalid command", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.total++
	s.byMatch[cmd.MatchID]++
	attempts := s.byMatch[cmd.MatchID]
	s.mu.Unlock()

	status := "applied"
	if attempts > 1 {
		status = "duplicate"
	}
	s.commands.WithLabelValues(status).Inc()
	// El evento de origen no viaja en el comando: el matchId es la clave para seguirlo en los logs.
	s.log.Info("command", "stage", "command", "result", map[string]string{"applied": "ok", "duplicate": "duplicate"}[status],
		"matchId", cmd.MatchID, "attempts", attempts, "kwh", cmd.KWh)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "matchId": cmd.MatchID, "attempts": attempts})
}

// Snapshot devuelve el estado actual.
func (s *Server) Snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	by := make(map[string]int, len(s.byMatch))
	for k, v := range s.byMatch {
		by[k] = v
	}
	return State{Total: s.total, Distinct: len(s.byMatch), Duplicates: s.total - len(s.byMatch), Rejected: s.rejected, ByMatchID: by}
}

func (s *Server) getState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Snapshot())
}

// reset borra el estado; solo existe con EXPERIMENTS=1.
func (s *Server) reset(w http.ResponseWriter, r *http.Request) {
	if !s.experiments {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	s.byMatch = map[string]int{}
	s.total, s.rejected = 0, 0
	s.mu.Unlock()
	s.log.Info("state reset", "stage", "command", "result", "ok")
	w.WriteHeader(http.StatusNoContent)
}

// Run arranca el servicio y bloquea hasta que ctx se cancele.
func Run(ctx context.Context, getenv func(string) string) error {
	cfg, err := LoadConfig(getenv)
	if err != nil {
		return err
	}
	svc := telemetry.New(telemetry.InfoFromEnv(serviceName), slog.Default())
	srv := NewServer(cfg.CommandKey, cfg.Experiments, svc.Logger, svc.Reg)
	svc.AddState("device", func(context.Context) any {
		st := srv.Snapshot()
		st.ByMatchID = nil // el detalle va en GET /state
		return st
	})

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- svc.Serve(ctx, cfg.OpsAddr) }()
	go func() { errc <- httpSrv.ListenAndServe() }()
	svc.Logger.Info("device-sim started", "stage", "start", "result", "ok", "addr", cfg.HTTPAddr, "experiments", cfg.Experiments)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
