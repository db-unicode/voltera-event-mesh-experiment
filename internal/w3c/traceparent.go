// Package w3c genera cabeceras traceparent (W3C Trace Context) sin SDK de OpenTelemetry.
package w3c

import (
	"crypto/rand"
	"encoding/hex"
)

// NewTraceparent devuelve "00-<trace-id 32 hex>-<span-id 16 hex>-01" con ids aleatorios no nulos.
func NewTraceparent() string {
	var b [24]byte
	for {
		_, _ = rand.Read(b[:])
		if !allZero(b[:16]) && !allZero(b[16:]) {
			break
		}
	}
	return "00-" + hex.EncodeToString(b[:16]) + "-" + hex.EncodeToString(b[16:]) + "-01"
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
