// Package testutil da acceso opt-in a Postgres y Redis reales en los tests de
// integración: TEST_POSTGRES_DSN y TEST_REDIS_ADDR (t.Skip si faltan).
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Postgres devuelve un pool aislado en un schema temporal (search_path) que se borra
// al terminar el test. Salta el test si TEST_POSTGRES_DSN no está definido.
func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN no definido: integración con Postgres omitida")
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "t_" + hex.EncodeToString(b)
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return pool
}

// Redis devuelve un cliente cuyas keys llevan un prefijo único (el test debe usarlo:
// ver el segundo valor) y las borra al terminar. Salta si TEST_REDIS_ADDR no está definido.
// TEST_REDIS_PASSWORD es opcional.
func Redis(t *testing.T) (client *redis.Client, prefix string) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR no definido: integración con Redis omitida")
	}
	client = redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("TEST_REDIS_PASSWORD"), DialTimeout: 5 * time.Second})
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	prefix = "t:" + hex.EncodeToString(b) + ":"
	require.NoError(t, client.Ping(context.Background()).Err())
	t.Cleanup(func() {
		ctx := context.Background()
		keys, _ := client.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		_ = client.Close()
	})
	return client, prefix
}
