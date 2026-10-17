package failpoint

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type exitRecorder struct {
	mu    sync.Mutex
	codes []int
}

func (e *exitRecorder) fn(c int) { e.mu.Lock(); e.codes = append(e.codes, c); e.mu.Unlock() }

func TestOffByDefault(t *testing.T) {
	r := New()
	require.False(t, r.Hit(context.Background(), "x"))
	require.Empty(t, r.Snapshot())
}

func TestCrashOnce(t *testing.T) {
	ex := &exitRecorder{}
	r := New(WithExit(ex.fn))
	require.NoError(t, r.Set("a", "crash-once"))
	r.Hit(context.Background(), "a")
	require.Equal(t, []int{137}, ex.codes)
	r.Hit(context.Background(), "a")
	require.Len(t, ex.codes, 1, "agotado tras el primer disparo")
}

func TestCrashAfterN(t *testing.T) {
	ex := &exitRecorder{}
	r := New(WithExit(ex.fn))
	require.NoError(t, r.Load("a=crash-after:3"))
	for range 3 {
		r.Hit(context.Background(), "a")
	}
	require.Empty(t, ex.codes)
	r.Hit(context.Background(), "a")
	require.Equal(t, []int{137}, ex.codes)
}

func TestCrashMarkerSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ex := &exitRecorder{}
	r1 := New(WithExit(ex.fn), WithStateDir(dir))
	require.NoError(t, r1.Load("a=crash-after:1"))
	r1.Hit(context.Background(), "a")
	r1.Hit(context.Background(), "a")
	require.Len(t, ex.codes, 1)
	require.FileExists(t, filepath.Join(dir, "a.fired"))

	// "Reinicio": un registro nuevo con el mismo env y directorio no vuelve a matar.
	r2 := New(WithExit(ex.fn), WithStateDir(dir))
	require.NoError(t, r2.Load("a=crash-after:1"))
	for range 5 {
		r2.Hit(context.Background(), "a")
	}
	require.Len(t, ex.codes, 1)

	// Un Set explícito (admin) rearma.
	require.NoError(t, r2.Set("a", "crash-once"))
	r2.Hit(context.Background(), "a")
	require.Len(t, ex.codes, 2)
}

func TestMarkerIgnoredIfModeChanged(t *testing.T) {
	dir := t.TempDir()
	ex := &exitRecorder{}
	r1 := New(WithExit(ex.fn), WithStateDir(dir))
	require.NoError(t, r1.Load("a=crash-once"))
	r1.Hit(context.Background(), "a")
	r2 := New(WithExit(ex.fn), WithStateDir(dir))
	require.NoError(t, r2.Load("a=crash-after:0"))
	r2.Hit(context.Background(), "a")
	require.Len(t, ex.codes, 2)
}

func TestDelay(t *testing.T) {
	r := New()
	require.NoError(t, r.Set("d", "delay_ms:60"))
	start := time.Now()
	require.False(t, r.Hit(context.Background(), "d"))
	require.GreaterOrEqual(t, time.Since(start), 55*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start = time.Now()
	r.Hit(ctx, "d")
	require.Less(t, time.Since(start), 40*time.Millisecond, "el contexto cancelado corta el retardo")
}

func TestDrop(t *testing.T) {
	r := New()
	require.NoError(t, r.Set("p", "drop"))
	require.True(t, r.Hit(context.Background(), "p"))
	require.NoError(t, r.Set("p", "off"))
	require.False(t, r.Hit(context.Background(), "p"))
	require.Empty(t, r.Snapshot())
}

func TestDropKeys(t *testing.T) {
	r := New()
	require.NoError(t, r.Set("p", "drop-keys:m-1;m-2"))
	require.True(t, r.HitKey(context.Background(), "p", "m-1"))
	require.True(t, r.HitKey(context.Background(), "p", "m-2"))
	require.False(t, r.HitKey(context.Background(), "p", "m-3"))
}

func TestInvalidModes(t *testing.T) {
	r := New()
	for _, m := range []string{"boom", "crash-after", "crash-after:x", "crash-after:-1", "delay_ms:", "drop-keys:"} {
		require.Error(t, r.Set("a", m), m)
	}
	require.Error(t, r.Set("", "drop"))
	require.Error(t, r.Load("sin-igual"))
}

func TestLoadMultiple(t *testing.T) {
	r := New()
	require.NoError(t, r.Load("a=crash-after:200, b=delay_ms:5,,c=drop"))
	require.Equal(t, map[string]string{"a": "crash-after:200", "b": "delay_ms:5", "c": "drop"}, r.Snapshot())
}

func TestHandler(t *testing.T) {
	r := New()
	post := func(h http.Handler, target string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, target, nil))
		return w
	}
	require.Equal(t, http.StatusNotFound, post(r.Handler(false), "/admin/failpoint?name=a&mode=drop").Code)
	require.Empty(t, r.Snapshot())

	require.Equal(t, http.StatusOK, post(r.Handler(true), "/admin/failpoint?name=a&mode=crash-after:5").Code)
	require.Equal(t, "crash-after:5", r.Snapshot()["a"])
	require.Equal(t, http.StatusBadRequest, post(r.Handler(true), "/admin/failpoint?name=a&mode=zzz").Code)

	w := httptest.NewRecorder()
	r.Handler(true).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/failpoint", nil))
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestFromEnv(t *testing.T) {
	t.Setenv("FAILPOINTS", "x=drop")
	t.Setenv("FAILPOINT_STATE_DIR", t.TempDir())
	r, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "drop", r.Snapshot()["x"])
	t.Setenv("FAILPOINTS", "x=bad")
	_, err = FromEnv()
	require.Error(t, err)
}

func TestExperiments(t *testing.T) {
	t.Setenv("EXPERIMENTS", "")
	require.False(t, Experiments())
	t.Setenv("EXPERIMENTS", "1")
	require.True(t, Experiments())
}
