// Package view contiene la lógica del probe del spike S7: orden de versiones, TTL
// real, expiración y recuperación tras FLUSHALL sobre la vista Redis.
package view

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

// Config del probe (env: REDIS_ADDR, REDIS_PASSWORD, VIEW_TTL_S, SHORT_TTL_S, KEY_PREFIX).
type Config struct {
	Addr     string
	Password string
	TTL      time.Duration // TTL de la vista (VIEW_TTL_S, 150 s)
	ShortTTL time.Duration // TTL de la prueba de expiración (10 s)
	Prefix   string
}

// ConfigFromEnv lee y valida la configuración.
func ConfigFromEnv() (Config, error) {
	c := Config{Addr: os.Getenv("REDIS_ADDR"), Password: os.Getenv("REDIS_PASSWORD"), Prefix: os.Getenv("KEY_PREFIX")}
	if c.Addr == "" {
		return c, errors.New("REDIS_ADDR is required")
	}
	if c.Prefix == "" {
		c.Prefix = "s07:"
	}
	var err error
	if c.TTL, err = envSeconds("VIEW_TTL_S", 150); err != nil {
		return c, err
	}
	if c.ShortTTL, err = envSeconds("SHORT_TTL_S", 10); err != nil {
		return c, err
	}
	return c, nil
}

func envSeconds(name string, def int) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return time.Duration(def) * time.Second, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be an integer >= 1", name)
	}
	return time.Duration(n) * time.Second, nil
}

type reporter struct {
	w      io.Writer
	failed int
}

// report imprime "CHECK PASS|FAIL <nombre> | <detalle>" (run.sh lo parsea).
func (r *reporter) report(ok bool, name, format string, args ...any) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		r.failed++
	}
	_, _ = fmt.Fprintf(r.w, "CHECK %s %s | %s\n", status, name, fmt.Sprintf(format, args...))
}

func within(got, want, tol time.Duration) bool {
	return math.Abs(float64(got-want)) <= float64(tol)
}

// Run ejecuta los checks de S7 y devuelve cuántos fallaron. ATENCIÓN: ejecuta FLUSHALL.
func Run(ctx context.Context, cfg Config, w io.Writer) (int, error) {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.Addr, Password: cfg.Password})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return 0, fmt.Errorf("redis not reachable: %w", err)
	}
	reg := prometheus.NewRegistry()
	store := view.New(rdb, reg)
	r := &reporter{w: w}
	defer cleanup(rdb, cfg.Prefix)

	key := cfg.Prefix + "m-0042"
	declare := func(v int64, side string, kwh float64, ttl time.Duration) (view.Result, error) {
		return store.Upsert(ctx, key, v, map[string]any{"side": side, "kwh": kwh, "zone": "z1"}, ttl)
	}

	// 1-3: orden invertido.
	res, err := declare(5, "SURPLUS", 4.2, cfg.TTL)
	if err != nil {
		return r.failed, err
	}
	r.report(res == view.Applied, "v5 se aplica", "resultado=%s", res)

	res, err = declare(3, "DEFICIT", 9.9, cfg.TTL)
	if err != nil {
		return r.failed, err
	}
	r.report(res == view.Stale, "v3 tras v5 devuelve stale", "resultado=%s", res)

	e, err := store.Get(ctx, key)
	if err != nil {
		return r.failed, err
	}
	r.report(e.Version == 5 && e.Fields["side"] == "SURPLUS" && e.Fields["kwh"] == "4.2",
		"la vista conserva v5", "version=%d side=%s kwh=%s", e.Version, e.Fields["side"], e.Fields["kwh"])

	// 4: TTL al escribir (v6 renueva el TTL).
	if _, err = declare(6, "SURPLUS", 4.3, cfg.TTL); err != nil {
		return r.failed, err
	}
	e, err = store.Get(ctx, key)
	if err != nil {
		return r.failed, err
	}
	r.report(within(e.TTL, cfg.TTL, time.Second), "TTL observado = TTL +/- 1 s",
		"ttl_configurado=%s ttl_observado=%s", cfg.TTL, e.TTL.Round(time.Millisecond))

	// 5: un stale no renueva el TTL.
	before := e.TTL
	time.Sleep(1100 * time.Millisecond)
	if res, err = declare(2, "DEFICIT", 1, cfg.TTL); err != nil {
		return r.failed, err
	}
	e, err = store.Get(ctx, key)
	if err != nil {
		return r.failed, err
	}
	r.report(res == view.Stale && e.TTL < before, "un stale no renueva el TTL",
		"resultado=%s ttl_antes=%s ttl_despues=%s", res, before.Round(time.Millisecond), e.TTL.Round(time.Millisecond))

	// 6: expiración con TTL corto.
	expKey := cfg.Prefix + "m-expire"
	if res, err = store.Upsert(ctx, expKey, 1, map[string]any{"side": "SURPLUS"}, cfg.ShortTTL); err != nil {
		return r.failed, err
	}
	t0 := time.Now()
	var gone time.Duration
	for {
		_, err = store.Get(ctx, expKey)
		if errors.Is(err, view.ErrNotFound) {
			gone = time.Since(t0)
			break
		}
		if err != nil {
			return r.failed, err
		}
		if time.Since(t0) > cfg.ShortTTL+10*time.Second {
			gone = time.Since(t0)
			break
		}
		select {
		case <-ctx.Done():
			return r.failed, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	r.report(res == view.Applied && within(gone, cfg.ShortTTL, 2*time.Second), "la key expira a TTL +/- 2 s",
		"ttl=%s desaparece_tras=%s", cfg.ShortTTL, gone.Round(time.Millisecond))

	// 7-8: FLUSHALL y repoblado.
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		return r.failed, err
	}
	_, err = store.Get(ctx, key)
	r.report(errors.Is(err, view.ErrNotFound), "FLUSHALL vacía la vista", "get_err=%v", err)

	res, err = declare(7, "DEFICIT", 2.5, cfg.TTL)
	if err != nil {
		return r.failed, err
	}
	e, gerr := store.Get(ctx, key)
	r.report(res == view.Applied && gerr == nil && e.Version == 7 && e.Fields["side"] == "DEFICIT",
		"una nueva declaración repuebla la vista", "resultado=%s get_err=%v", res, gerr)

	// 9: métricas.
	applied, stale := counter(reg, "applied"), counter(reg, "stale")
	r.report(applied == 4 && stale == 2, "view_writes_total cuenta applied y stale",
		"applied=%v (esperado 4) stale=%v (esperado 2)", applied, stale)
	return r.failed, nil
}

func cleanup(rdb *redis.Client, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if keys, err := rdb.Keys(ctx, prefix+"*").Result(); err == nil && len(keys) > 0 {
		rdb.Del(ctx, keys...)
	}
}

func counter(reg *prometheus.Registry, result string) float64 {
	mfs, err := reg.Gather()
	if err != nil {
		return -1
	}
	for _, mf := range mfs {
		if mf.GetName() != "view_writes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelValue(m, "result") == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}
