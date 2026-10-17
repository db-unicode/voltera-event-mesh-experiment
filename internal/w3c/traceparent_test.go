package w3c

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

func TestNewTraceparentIsValidAndUnique(t *testing.T) {
	re := regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)
	seen := map[string]bool{}
	for range 1000 {
		tp := NewTraceparent()
		require.Regexp(t, re, tp)
		require.Len(t, events.TraceID(tp), 32, "events.TraceID lo entiende")
		require.False(t, seen[tp])
		seen[tp] = true
	}
}
