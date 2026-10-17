// Package outbox contiene la lógica del probe del spike S6: inserta filas de negocio
// con su evento en la outbox y corre el relay contra un Publisher que escribe en una
// tabla sink sin unique, para contar duplicados.
package outbox

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/outbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Topic usado por el probe.
const Topic = "s06-positions"

// Config del probe (env: DATABASE_URL, ROWS, BATCH_SIZE, SEED_DELAY_MS, TIMEOUT_S).
type Config struct {
	DatabaseURL string
	Rows        int
	BatchSize   int
	SeedDelay   time.Duration // pausa entre filas, para que el crash caiga a mitad de la siembra
	Timeout     time.Duration
}

// ConfigFromEnv lee y valida la configuración.
func ConfigFromEnv() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("DATABASE_URL")}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	var err error
	if c.Rows, err = envInt("ROWS", 1000); err != nil {
		return c, err
	}
	if c.BatchSize, err = envInt("BATCH_SIZE", 100); err != nil {
		return c, err
	}
	delay, err := envInt("SEED_DELAY_MS", 2)
	if err != nil {
		return c, err
	}
	timeout, err := envInt("TIMEOUT_S", 120)
	if err != nil {
		return c, err
	}
	c.SeedDelay, c.Timeout = time.Duration(delay)*time.Millisecond, time.Duration(timeout)*time.Second
	if c.Rows < 1 || c.BatchSize < 1 {
		return c, errors.New("ROWS and BATCH_SIZE must be >= 1")
	}
	return c, nil
}

func envInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}

// Result es el resumen que el probe imprime al terminar (run.sh lo verifica además con psql).
type Result struct {
	Business     int64 `json:"business"`
	Outbox       int64 `json:"outbox"`
	Pending      int64 `json:"pending"`
	SinkTotal    int64 `json:"sinkTotal"`
	SinkDistinct int64 `json:"sinkDistinct"`
	Duplicates   int64 `json:"duplicates"`
}

// sinkPublisher escribe cada mensaje en sink con su propia conexión (fuera de la
// transacción del relay): el "broker" es la tabla.
type sinkPublisher struct{ pool *pgxpool.Pool }

func (s sinkPublisher) Publish(ctx context.Context, m outbox.Message) error {
	_, err := s.pool.Exec(ctx, "INSERT INTO sink (event_id) VALUES ($1)", m.EventID)
	return err
}

// Connect abre el pool reintentando hasta que la base acepte conexiones.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	for {
		err = pool.Ping(ctx)
		if err == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("database not reachable: %w (last: %v)", ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// Run migra, siembra (reanudable) y publica hasta que la outbox queda vacía.
func Run(ctx context.Context, cfg Config, svc *telemetry.Service, fp *failpoint.Registry) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	pool, err := Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return Result{}, err
	}
	defer pool.Close()
	return runWithPool(ctx, pool, cfg, svc, fp)
}

func runWithPool(ctx context.Context, pool *pgxpool.Pool, cfg Config, svc *telemetry.Service, fp *failpoint.Registry) (Result, error) {
	if err := migrate.Up(ctx, pool, outbox.Migrations(),
		migrate.Source{Name: "s06", FS: migrationsFS, Dir: "migrations"}); err != nil {
		return Result{}, err
	}

	relay := outbox.NewRelay(pool, sinkPublisher{pool}, outbox.Config{
		BatchSize: cfg.BatchSize, Telemetry: svc, Failpoints: fp,
	})
	svc.AddState("outbox", relay.StateProvider())
	svc.AddReadyCheck("postgres", pool.Ping)

	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	go func() { defer close(relayDone); _ = relay.Run(relayCtx) }()
	defer func() { stopRelay(); <-relayDone }()

	if err := seed(ctx, pool, relay, cfg, svc); err != nil {
		return Result{}, err
	}

	for { // esperar a que la outbox se vacíe
		st, err := relay.Stats(ctx)
		if err != nil {
			return Result{}, err
		}
		if st.Pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return Result{}, fmt.Errorf("timeout draining outbox (pending=%d): %w", st.Pending, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	return Verify(ctx, pool)
}

// seed inserta las filas de negocio que falten; cada una con su evento en la misma
// transacción. Tras un reinicio continúa donde quedó (ON CONFLICT DO NOTHING).
func seed(ctx context.Context, pool *pgxpool.Pool, relay *outbox.Relay, cfg Config, svc *telemetry.Service) error {
	inserted := 0
	for i := 1; i <= cfg.Rows; i++ {
		ok, err := seedRow(ctx, pool, i)
		if err != nil {
			return fmt.Errorf("seed row %d: %w", i, err)
		}
		if ok {
			inserted++
			relay.Wake()
		}
		if cfg.SeedDelay > 0 {
			time.Sleep(cfg.SeedDelay)
		}
	}
	svc.Logger.Info("seed done", "stage", telemetry.StageCommit, "result", telemetry.ResultOK, "inserted", inserted, "rows", cfg.Rows)
	return nil
}

func seedRow(ctx context.Context, pool *pgxpool.Pool, i int) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	meter := fmt.Sprintf("m-%04d", i)
	tag, err := tx.Exec(ctx, "INSERT INTO business (id, meter_id, kwh) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", i, meter, float64(i%10)+0.5)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil // ya sembrada antes del reinicio
	}
	env, err := events.NewPositionDeclared("r1", 1, time.Now(), events.PositionDeclared{
		MeterID: meter, Side: events.SideSurplus, KWh: float64(i%10) + 0.5, Zone: "z1"})
	if err != nil {
		return false, err
	}
	if err := outbox.Append(ctx, tx, env, Topic, meter); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// Verify cuenta filas de negocio, outbox y sink.
func Verify(ctx context.Context, pool *pgxpool.Pool) (Result, error) {
	var r Result
	err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM business),
		(SELECT count(*) FROM outbox),
		(SELECT count(*) FROM outbox WHERE published_at IS NULL),
		(SELECT count(*) FROM sink),
		(SELECT count(DISTINCT event_id) FROM sink)`).
		Scan(&r.Business, &r.Outbox, &r.Pending, &r.SinkTotal, &r.SinkDistinct)
	r.Duplicates = r.SinkTotal - r.SinkDistinct
	return r, err
}

// Log escribe el resultado como una línea estructurada.
func (r Result) Log(l *slog.Logger) {
	l.Info("probe result", "stage", "result", "result", "done",
		"business", r.Business, "outbox", r.Outbox, "pending", r.Pending,
		"sinkTotal", r.SinkTotal, "sinkDistinct", r.SinkDistinct, "duplicates", r.Duplicates)
}
