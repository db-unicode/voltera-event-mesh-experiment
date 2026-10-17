package matching

import (
	"crypto/rand"
	"encoding/hex"
)

// NewTraceparent genera un traceparent W3C nuevo ("00-<traceId>-<spanId>-01"). El
// motor es el origen de la traza de un match: las posiciones de la vista no la llevan.
func NewTraceparent() string {
	var b [24]byte
	_, _ = rand.Read(b[:])
	return "00-" + hex.EncodeToString(b[:16]) + "-" + hex.EncodeToString(b[16:]) + "-01"
}
