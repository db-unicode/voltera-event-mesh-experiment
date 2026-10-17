package projection

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// El proyector no lee positions-db: su binario no debe enlazar ningún driver de Postgres.
// Equivale a `go list -deps ./cmd/market-projector | grep -c pgx` = 0.
func TestProjectorBinaryDoesNotLinkPostgres(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go no está en el PATH")
	}
	out, err := exec.Command(goBin, "list", "-deps", "github.com/db-unicode/voltera-event-mesh-experiment/cmd/market-projector").CombinedOutput()
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "go-redis", "sanity: la lista incluye las dependencias reales")
	for _, pkg := range strings.Fields(string(out)) {
		for _, banned := range []string{"pgx", "jackc", "lib/pq", "internal/outbox", "internal/migrate"} {
			require.NotContains(t, pkg, banned, "market-projector depende de %s", pkg)
		}
	}
}
