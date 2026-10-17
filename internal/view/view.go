// Package view es la vista de lectura en Redis (una key hash por entidad, p. ej. por
// medidor) con TTL y descarte atómico de versiones viejas.
package view

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// Result de un Upsert.
type Result string

const (
	Applied Result = "applied"
	Stale   Result = "stale"
)

// versionField guarda la versión dentro del hash; no puede usarse como campo de usuario.
const versionField = "_v"

// ErrNotFound indica que la key no existe (nunca escrita, expirada o borrada).
var ErrNotFound = errors.New("view: key not found")

// upsertScript aplica el estado solo si la versión entrante es mayor que la guardada
// (igual = duplicado = stale). Reemplaza el hash completo (transferencia de estado:
// los campos que ya no vienen desaparecen) y renueva el TTL. Todo en un único paso
// atómico en el servidor. KEYS[1]=key; ARGV[1]=versión, ARGV[2]=ttl en ms, ARGV[3..]=campo,valor.
var upsertScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], '_v')
if cur and tonumber(cur) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], '_v', ARGV[1], unpack(ARGV, 3))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

// Entry es el contenido de una key de la vista.
type Entry struct {
	Key     string
	Version int64
	Fields  map[string]string
	TTL     time.Duration // restante; -1 si la key no tiene TTL
}

// Metrics son las métricas de la vista.
type Metrics struct {
	Writes *prometheus.CounterVec // view_writes_total{result=applied|stale|expired}
}

// NewMetrics crea view_writes_total y lo registra en reg (si no es nil).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{Writes: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "view_writes_total", Help: "Escrituras a la vista Redis por resultado."}, []string{"result"})}
	if reg != nil {
		reg.MustRegister(m.Writes)
	}
	return m
}

// Store accede a la vista.
type Store struct {
	rdb     redis.UniversalClient
	metrics *Metrics
}

// New crea el Store. reg puede ser nil (sin métricas registradas).
func New(rdb redis.UniversalClient, reg prometheus.Registerer) *Store {
	return &Store{rdb: rdb, metrics: NewMetrics(reg)}
}

// Upsert escribe fields en key si version es mayor que la guardada y fija el TTL.
// Devuelve Applied o Stale (versión igual o menor: la vista no cambia, el TTL tampoco).
func (s *Store) Upsert(ctx context.Context, key string, version int64, fields map[string]any, ttl time.Duration) (Result, error) {
	if ttl < time.Millisecond {
		return "", fmt.Errorf("view: ttl must be >= 1ms, got %v", ttl)
	}
	args := make([]any, 0, 2+2*len(fields))
	args = append(args, version, ttl.Milliseconds())
	for k, v := range fields {
		if k == versionField {
			return "", fmt.Errorf("view: field name %q is reserved", versionField)
		}
		args = append(args, k, v)
	}
	n, err := upsertScript.Run(ctx, s.rdb, []string{key}, args...).Int()
	if err != nil {
		return "", fmt.Errorf("view: upsert %s: %w", key, err)
	}
	res := Stale
	if n == 1 {
		res = Applied
	}
	s.metrics.Writes.WithLabelValues(string(res)).Inc()
	return res, nil
}

// Get lee una key. Devuelve ErrNotFound si no existe.
func (s *Store) Get(ctx context.Context, key string) (*Entry, error) {
	pipe := s.rdb.Pipeline()
	h := pipe.HGetAll(ctx, key)
	t := pipe.PTTL(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("view: get %s: %w", key, err)
	}
	return buildEntry(key, h.Val(), t.Val())
}

func buildEntry(key string, raw map[string]string, ttl time.Duration) (*Entry, error) {
	if len(raw) == 0 {
		return nil, ErrNotFound
	}
	v, err := strconv.ParseInt(raw[versionField], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("view: %s has invalid version %q", key, raw[versionField])
	}
	delete(raw, versionField)
	if ttl < 0 {
		ttl = -1
	}
	return &Entry{Key: key, Version: v, Fields: raw, TTL: ttl}, nil
}

var globEscaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

// ScanByPrefix devuelve todas las entradas cuya key empieza por prefix (SCAN, no
// bloquea el servidor; no es una foto atómica: puede omitir keys que cambian durante el barrido).
func (s *Store) ScanByPrefix(ctx context.Context, prefix string) ([]Entry, error) {
	pattern := globEscaper.Replace(prefix) + "*"
	seen := map[string]struct{}{}
	var keys []string
	var cursor uint64
	for {
		batch, next, err := s.rdb.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return nil, fmt.Errorf("view: scan %q: %w", prefix, err)
		}
		for _, k := range batch {
			if _, dup := seen[k]; !dup { // SCAN puede repetir keys
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := s.rdb.Pipeline()
	hs := make([]*redis.MapStringStringCmd, len(keys))
	ts := make([]*redis.DurationCmd, len(keys))
	for i, k := range keys {
		hs[i] = pipe.HGetAll(ctx, k)
		ts[i] = pipe.PTTL(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("view: scan %q: %w", prefix, err)
	}
	out := make([]Entry, 0, len(keys))
	for i, k := range keys {
		e, err := buildEntry(k, hs[i].Val(), ts[i].Val())
		if errors.Is(err, ErrNotFound) {
			continue // expiró entre el SCAN y la lectura
		}
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, nil
}
