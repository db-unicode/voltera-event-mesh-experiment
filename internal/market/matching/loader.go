package matching

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RulesFileName es el archivo dentro del directorio montado del ConfigMap.
const RulesFileName = "rules.yaml"

// RulesLoader mantiene las reglas vigentes y las recarga cuando cambia el archivo.
// La recarga es atómica: Current devuelve siempre un conjunto completo y compilado.
// Si el archivo nuevo no compila se conserva el anterior y se registra el error.
type RulesLoader struct {
	path    string
	log     *slog.Logger
	cur     atomic.Pointer[Rules]
	lastErr atomic.Pointer[string]
	loadedA atomic.Pointer[time.Time]
	reloads *prometheus.CounterVec
	// lastSeen es el sha256 del último contenido leído (válido o no), para no repetir
	// el mismo error en cada poll.
	lastSeen string
}

// NewRulesLoader lee el archivo del directorio dir. Falla si las reglas iniciales no
// compilan (el servicio no arranca sin reglas válidas).
func NewRulesLoader(dir string, log *slog.Logger, reg prometheus.Registerer) (*RulesLoader, error) {
	l := &RulesLoader{path: filepath.Join(dir, RulesFileName), log: log,
		reloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "matching_rules_reload_total", Help: "Recargas de las reglas por resultado."}, []string{"result"})}
	l.reloads.WithLabelValues("ok")
	l.reloads.WithLabelValues("error")
	if reg != nil {
		reg.MustRegister(l.reloads)
	}
	if _, err := l.Reload(); err != nil {
		return nil, err
	}
	return l, nil
}

// Current devuelve las reglas vigentes.
func (l *RulesLoader) Current() *Rules { return l.cur.Load() }

// Reload lee el archivo y, si su contenido cambió, lo compila y lo publica. Devuelve
// true si cambiaron las reglas vigentes.
func (l *RulesLoader) Reload() (bool, error) {
	data, err := os.ReadFile(l.path)
	if err != nil {
		l.log.Warn("rules file unreadable; keeping previous rules", "stage", "start", "result", "error", "error", err)
		return false, fmt.Errorf("rules: read %s: %w", l.path, err)
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if hash == l.lastSeen {
		return false, nil
	}
	l.lastSeen = hash
	rules, err := ParseRules(bytes.Clone(data))
	if err != nil {
		l.reloads.WithLabelValues("error").Inc()
		msg := err.Error()
		l.lastErr.Store(&msg)
		if prev := l.cur.Load(); prev != nil {
			l.log.Error("rules reload failed; keeping previous rules", "stage", "start", "result", "error",
				"keeping", prev.Version, "error", err)
		}
		return false, err
	}
	l.cur.Store(rules)
	now := time.Now().UTC()
	l.loadedA.Store(&now)
	l.lastErr.Store(nil)
	l.reloads.WithLabelValues("ok").Inc()
	l.log.Info("rules loaded", "stage", "start", "result", "ok", "rules", rules.Version, "params", rules.Params)
	return true, nil
}

// Run sondea el archivo cada interval hasta que ctx se cancele. Los errores de
// compilación se registran y se conservan las reglas anteriores.
func (l *RulesLoader) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = l.Reload() // los errores ya se registran dentro; se conservan las reglas anteriores
		}
	}
}

// RulesState es la sección "rules" de /debug/state.
type RulesState struct {
	Version         string         `json:"version"`
	LoadedAt        *time.Time     `json:"loadedAt,omitempty"`
	Params          map[string]any `json:"params"`
	Eligible        string         `json:"eligible"`
	Pair            string         `json:"pair"`
	LastReloadError string         `json:"lastReloadError,omitempty"`
}

// State devuelve el estado para /debug/state.
func (l *RulesLoader) State() RulesState {
	r := l.cur.Load()
	s := RulesState{Version: r.Version, LoadedAt: l.loadedA.Load(), Params: r.Params, Eligible: r.EligibleSrc, Pair: r.PairSrc}
	if e := l.lastErr.Load(); e != nil {
		s.LastReloadError = *e
	}
	return s
}
