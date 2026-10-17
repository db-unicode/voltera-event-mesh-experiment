package migrate

import (
	"context"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
)

func TestLoadOrdersAndValidates(t *testing.T) {
	fsys := fstest.MapFS{
		"sql/0002_second.sql": {Data: []byte("SELECT 2")},
		"sql/0001_first.sql":  {Data: []byte("SELECT 1")},
		"sql/README.md":       {Data: []byte("ignorado")},
	}
	ms, err := Load(Source{Name: "x", FS: fsys, Dir: "sql"})
	require.NoError(t, err)
	require.Len(t, ms, 2)
	require.Equal(t, 1, ms[0].Version)
	require.Equal(t, "first", ms[0].Name)
	require.Equal(t, 2, ms[1].Version)
	require.NotEqual(t, ms[0].Checksum, ms[1].Checksum)

	_, err = Load(Source{Name: "x", FS: fstest.MapFS{"bad.sql": {Data: []byte("x")}}})
	require.Error(t, err)
	_, err = Load(Source{Name: "x", FS: fstest.MapFS{"0000_zero.sql": {Data: []byte("x")}}})
	require.Error(t, err)
	_, err = Load(Source{Name: "x", FS: fstest.MapFS{"1_a.sql": {Data: []byte("x")}, "0001_b.sql": {Data: []byte("x")}}})
	require.Error(t, err, "misma versión dos veces")
}

func TestUpIntegration(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_create.sql": {Data: []byte("CREATE TABLE widgets (id int PRIMARY KEY); INSERT INTO widgets VALUES (1);")},
		"0002_add.sql":    {Data: []byte("ALTER TABLE widgets ADD COLUMN name text")},
	}
	src := Source{Name: "widgets", FS: fsys}

	require.NoError(t, Up(ctx, pool, src))
	require.NoError(t, Up(ctx, pool, src), "idempotente")

	var n int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations WHERE source='widgets'").Scan(&n))
	require.Equal(t, 2, n)
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM widgets").Scan(&n))
	require.Equal(t, 1, n, "0001 no se reaplica")

	// Una migración nueva se aplica sola.
	fsys["0003_more.sql"] = &fstest.MapFile{Data: []byte("INSERT INTO widgets (id, name) VALUES (2, 'b')")}
	require.NoError(t, Up(ctx, pool, src))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM widgets").Scan(&n))
	require.Equal(t, 2, n)
}

func TestUpDetectsModifiedAndFailedMigrations(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	fsys := fstest.MapFS{"0001_create.sql": {Data: []byte("CREATE TABLE a (id int)")}}
	require.NoError(t, Up(ctx, pool, Source{Name: "a", FS: fsys}))

	fsys["0001_create.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE a (id bigint)")}
	require.ErrorContains(t, Up(ctx, pool, Source{Name: "a", FS: fsys}), "modified")

	// Una migración que falla se revierte entera y no deja registro.
	bad := fstest.MapFS{"0001_bad.sql": {Data: []byte("CREATE TABLE b (id int); SELECT * FROM no_such_table;")}}
	require.Error(t, Up(ctx, pool, Source{Name: "b", FS: bad}))
	var exists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('b') IS NOT NULL").Scan(&exists))
	require.False(t, exists)
}

func TestUpSourcesAreIndependentAndConcurrentSafe(t *testing.T) {
	pool := testutil.Postgres(t)
	ctx := context.Background()
	s1 := Source{Name: "one", FS: fstest.MapFS{"0001_t.sql": {Data: []byte("CREATE TABLE one (id int)")}}}
	s2 := Source{Name: "two", FS: fstest.MapFS{"0001_t.sql": {Data: []byte("CREATE TABLE two (id int)")}}}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() { errs <- Up(ctx, pool, s1, s2) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var n int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&n))
	require.Equal(t, 2, n)
}
