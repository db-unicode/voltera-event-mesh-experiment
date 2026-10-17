// Package migrate aplica migraciones SQL versionadas y embebidas (embed.FS) al arrancar.
//
// Migrador propio y mínimo en lugar de goose: solo necesitamos "hacia adelante",
// cada librería (outbox, inbox) y cada servicio aporta su propia fuente al mismo
// esquema, y goose exige database/sql (pgx/stdlib) y arrastra dependencias que no
// usamos. Son ~150 líneas con checksum, lock y una transacción por migración.
//
// Convención: archivos "NNNN_nombre.sql" (NNNN entero > 0) en el directorio de cada
// Source. Cada archivo corre en una transacción junto con su fila en schema_migrations.
// Una migración aplicada no se edita: el checksum distinto aborta el arranque.
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// lockID es la clave del advisory lock que serializa a varias réplicas arrancando a la vez.
const lockID int64 = 0x766f6c74 // "volt"

// Source es un conjunto de migraciones con nombre propio (cada uno lleva su propia
// numeración en schema_migrations).
type Source struct {
	Name string // "outbox", "inbox", "positions"...
	FS   fs.FS
	Dir  string // directorio dentro de FS ("" o "." = raíz)
}

// Migration es una migración parseada.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string
}

// Load lee y ordena las migraciones de una fuente.
func Load(src Source) ([]Migration, error) {
	dir := src.Dir
	if dir == "" {
		dir = "."
	}
	entries, err := fs.ReadDir(src.FS, dir)
	if err != nil {
		return nil, fmt.Errorf("migrate: read %s/%s: %w", src.Name, dir, err)
	}
	var out []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		num, name, ok := strings.Cut(strings.TrimSuffix(e.Name(), ".sql"), "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || v <= 0 {
			return nil, fmt.Errorf("migrate: %s: file %q must be NNNN_name.sql", src.Name, e.Name())
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrate: %s: version %d in both %q and %q", src.Name, v, prev, e.Name())
		}
		seen[v] = e.Name()
		b, err := fs.ReadFile(src.FS, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(b)
		out = append(out, Migration{Version: v, Name: name, SQL: string(b), Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Up aplica, en orden, las migraciones pendientes de cada fuente. Es idempotente y
// segura con varias réplicas concurrentes (advisory lock).
func Up(ctx context.Context, pool *pgxpool.Pool, sources ...Source) error {
	plans := make([][]Migration, len(sources))
	for i, s := range sources {
		if s.Name == "" {
			return errors.New("migrate: source without name")
		}
		m, err := Load(s)
		if err != nil {
			return err
		}
		plans[i] = m
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}
	defer func() {
		// Contexto propio: si ctx se canceló, el lock se suelta igual al cerrar la sesión.
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lockID)
	}()

	// Tabla de control del propio migrador (único CREATE ... IF NOT EXISTS permitido).
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		source     text        NOT NULL,
		version    integer     NOT NULL,
		name       text        NOT NULL,
		checksum   text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		PRIMARY KEY (source, version)
	)`); err != nil {
		return fmt.Errorf("migrate: bootstrap: %w", err)
	}

	for i, src := range sources {
		if err := apply(ctx, conn.Conn(), src.Name, plans[i]); err != nil {
			return err
		}
	}
	return nil
}

func apply(ctx context.Context, conn *pgx.Conn, source string, migrations []Migration) error {
	rows, err := conn.Query(ctx, "SELECT version, checksum FROM schema_migrations WHERE source = $1", source)
	if err != nil {
		return fmt.Errorf("migrate: read state: %w", err)
	}
	applied := map[int]string{}
	maxApplied := 0
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			rows.Close()
			return err
		}
		applied[v] = sum
		maxApplied = max(maxApplied, v)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migrations {
		if sum, ok := applied[m.Version]; ok {
			if sum != m.Checksum {
				return fmt.Errorf("migrate: %s %04d_%s was modified after being applied", source, m.Version, m.Name)
			}
			continue
		}
		if m.Version < maxApplied {
			return fmt.Errorf("migrate: %s %04d_%s is older than applied version %d", source, m.Version, m.Name, maxApplied)
		}
		if err := applyOne(ctx, conn, source, m); err != nil {
			return err
		}
		maxApplied = m.Version
		slog.Info("migration applied", "stage", "migrate", "source", source, "version", m.Version, "name", m.Name)
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, source string, m Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("migrate: %s %04d_%s: %w", source, m.Version, m.Name, err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO schema_migrations (source, version, name, checksum) VALUES ($1, $2, $3, $4)",
		source, m.Version, m.Name, m.Checksum); err != nil {
		return fmt.Errorf("migrate: record %s %04d: %w", source, m.Version, err)
	}
	return tx.Commit(ctx)
}
