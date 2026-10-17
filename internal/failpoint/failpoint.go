// Package failpoint implementa puntos de fallo por nombre para los experimentos
// de caos. Sin configuración todos los failpoints están en "off" (coste: un lookup).
//
// Modos:
//
//	off              no hace nada
//	crash-once       el primer Hit termina el proceso con exit 137
//	crash-after:N    los primeros N Hit pasan; el N+1 termina el proceso
//	delay_ms:N       cada Hit duerme N ms
//	drop             cada Hit devuelve drop=true (el llamador descarta el mensaje)
//	drop-keys:k1,k2  HitKey devuelve drop=true solo para esas claves
//
// Activación: env FAILPOINTS="name=mode,name2=mode2" o, solo con EXPERIMENTS=1,
// POST /admin/failpoint?name=<n>&mode=<modo> (ver Handler).
//
// Un crash sobrevive al reinicio del contenedor: si FAILPOINT_STATE_DIR apunta a un
// directorio persistente entre reinicios (un emptyDir), el failpoint deja una marca
// y tras el reinicio queda agotado (no vuelve a matar el proceso). Sin ese
// directorio, crash-after:N vuelve a disparar tras N hits en cada reinicio.
package failpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ExitCode es el código de salida de un crash (128 + SIGKILL).
const ExitCode = 137

type kind int

const (
	kindOff kind = iota
	kindCrashOnce
	kindCrashAfter
	kindDelay
	kindDrop
	kindDropKeys
)

type point struct {
	mode  string
	kind  kind
	n     int64
	delay time.Duration
	keys  map[string]struct{}
	hits  int64
	spent bool
}

// Registry guarda los failpoints activos. Es seguro para uso concurrente.
type Registry struct {
	mu       sync.Mutex
	points   map[string]*point
	stateDir string
	exit     func(int)
}

// Option configura un Registry.
type Option func(*Registry)

// WithStateDir fija el directorio donde se guardan las marcas de crash ya disparado.
func WithStateDir(dir string) Option { return func(r *Registry) { r.stateDir = dir } }

// WithExit reemplaza os.Exit (tests).
func WithExit(fn func(int)) Option { return func(r *Registry) { r.exit = fn } }

// New crea un Registry vacío.
func New(opts ...Option) *Registry {
	r := &Registry{points: map[string]*point{}, exit: os.Exit}
	for _, o := range opts {
		o(r)
	}
	return r
}

// FromEnv crea un Registry con FAILPOINT_STATE_DIR y FAILPOINTS del entorno.
func FromEnv(opts ...Option) (*Registry, error) {
	if dir := os.Getenv("FAILPOINT_STATE_DIR"); dir != "" {
		opts = append([]Option{WithStateDir(dir)}, opts...)
	}
	r := New(opts...)
	return r, r.Load(os.Getenv("FAILPOINTS"))
}

// Experiments indica si EXPERIMENTS=1 (único caso en que se expone el endpoint admin).
func Experiments() bool { return os.Getenv("EXPERIMENTS") == "1" }

// Load aplica una lista "name=mode,name2=mode2". El modo drop-keys:a,b contiene
// comas; se escribe con ';' como separador de claves en esta forma: drop-keys:a;b.
func (r *Registry) Load(spec string) error {
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, mode, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("failpoint: invalid entry %q (want name=mode)", item)
		}
		if err := r.Set(strings.TrimSpace(name), strings.TrimSpace(mode)); err != nil {
			return err
		}
	}
	return nil
}

func parseMode(mode string) (*point, error) {
	p := &point{mode: mode}
	head, arg, hasArg := strings.Cut(mode, ":")
	switch head {
	case "off", "":
		p.kind, p.mode = kindOff, "off"
	case "crash-once":
		p.kind = kindCrashOnce
	case "drop":
		p.kind = kindDrop
	case "crash-after", "delay_ms":
		n, err := strconv.ParseInt(arg, 10, 64)
		if !hasArg || err != nil || n < 0 {
			return nil, fmt.Errorf("failpoint: mode %q needs a non-negative integer (%s:N)", mode, head)
		}
		if head == "crash-after" {
			p.kind, p.n = kindCrashAfter, n
		} else {
			p.kind, p.delay = kindDelay, time.Duration(n)*time.Millisecond
		}
	case "drop-keys":
		if !hasArg || arg == "" {
			return nil, fmt.Errorf("failpoint: mode %q needs keys (drop-keys:k1;k2)", mode)
		}
		p.kind, p.keys = kindDropKeys, map[string]struct{}{}
		for _, k := range strings.FieldsFunc(arg, func(r rune) bool { return r == ';' || r == ',' }) {
			p.keys[k] = struct{}{}
		}
	default:
		return nil, fmt.Errorf("failpoint: unknown mode %q", mode)
	}
	return p, nil
}

// Set fija el modo de un failpoint, reinicia su contador y rearma sus marcas de crash.
func (r *Registry) Set(name, mode string) error {
	if name == "" {
		return errors.New("failpoint: empty name")
	}
	p, err := parseMode(mode)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stateDir != "" {
		// Un Load inicial respeta la marca (el crash ya ocurrió); un Set posterior la borra.
		if _, loaded := r.points[name]; !loaded && r.fired(name, p.mode) {
			p.spent = true
			slog.Info("failpoint already fired before restart", "stage", "failpoint", "result", "spent", "failpoint", name, "mode", p.mode)
		} else {
			_ = os.Remove(r.markerPath(name))
		}
	}
	if p.kind == kindOff {
		delete(r.points, name)
		return nil
	}
	r.points[name] = p
	return nil
}

func (r *Registry) markerPath(name string) string {
	return filepath.Join(r.stateDir, name+".fired")
}

func (r *Registry) fired(name, mode string) bool {
	b, err := os.ReadFile(r.markerPath(name))
	return err == nil && string(b) == mode
}

// Hit evalúa el failpoint name. Aplica retardos, termina el proceso en los modos
// crash y devuelve true si el llamador debe descartar el mensaje (modo drop).
func (r *Registry) Hit(ctx context.Context, name string) (drop bool) {
	return r.hit(ctx, name, "")
}

// HitKey es como Hit pero para failpoints drop-keys: descarta solo si key está en la lista.
func (r *Registry) HitKey(ctx context.Context, name, key string) (drop bool) {
	return r.hit(ctx, name, key)
}

func (r *Registry) hit(ctx context.Context, name, key string) bool {
	r.mu.Lock()
	p, ok := r.points[name]
	if !ok || p.spent {
		r.mu.Unlock()
		return false
	}
	p.hits++
	hits, mode := p.hits, p.mode
	switch p.kind {
	case kindDelay:
		d := p.delay
		r.mu.Unlock()
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
		return false
	case kindDrop:
		r.mu.Unlock()
		return true
	case kindDropKeys:
		_, drop := p.keys[key]
		r.mu.Unlock()
		return drop
	case kindCrashOnce, kindCrashAfter:
		if p.kind == kindCrashAfter && hits <= p.n {
			r.mu.Unlock()
			return false
		}
		p.spent = true
		if r.stateDir != "" {
			_ = os.MkdirAll(r.stateDir, 0o755)
			_ = os.WriteFile(r.markerPath(name), []byte(mode), 0o644)
		}
		r.mu.Unlock()
		slog.Error("failpoint crash", "stage", "failpoint", "result", "crash", "failpoint", name, "mode", mode, "hits", hits)
		r.exit(ExitCode)
		return false // solo alcanzable con un exit de prueba
	}
	r.mu.Unlock()
	return false
}

// Snapshot devuelve name -> modo de los failpoints activos.
func (r *Registry) Snapshot() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.points))
	for name, p := range r.points {
		out[name] = p.mode
	}
	return out
}

// Handler implementa POST /admin/failpoint?name=<n>&mode=<modo>. Si enabled es
// false responde 404: sin EXPERIMENTS=1 el endpoint no existe.
func (r *Registry) Handler(enabled bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !enabled {
			http.NotFound(w, req)
			return
		}
		if req.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		name, mode := req.URL.Query().Get("name"), req.URL.Query().Get("mode")
		if err := r.Set(name, mode); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		slog.Info("failpoint set", "stage", "failpoint", "failpoint", name, "mode", mode)
		w.Header().Set("Content-Type", "application/json")
		snap := r.Snapshot()
		names := make([]string, 0, len(snap))
		for n := range snap {
			names = append(names, n)
		}
		sort.Strings(names)
		_ = json.NewEncoder(w).Encode(map[string]any{"failpoints": snap, "names": names})
	})
}
