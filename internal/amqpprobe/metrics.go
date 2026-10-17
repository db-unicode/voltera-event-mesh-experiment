package amqpprobe

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Metrics cuenta los mensajes consumidos y el estado de la conexión del proceso.
type Metrics struct {
	consumed  atomic.Uint64
	connected atomic.Bool
}

// IncConsumed suma un mensaje confirmado (ack).
func (m *Metrics) IncConsumed() { m.consumed.Add(1) }

// Consumed devuelve los mensajes confirmados hasta ahora.
func (m *Metrics) Consumed() uint64 { return m.consumed.Load() }

// SetConnected marca si hay una sesión AMQP activa.
func (m *Metrics) SetConnected(v bool) { m.connected.Store(v) }

// Handler sirve /metrics (texto Prometheus), /healthz (proceso vivo) y /readyz (conectado al broker).
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# HELP events_consumed_total Mensajes confirmados (ack) por este proceso.\n"+
			"# TYPE events_consumed_total counter\nevents_consumed_total %d\n", m.Consumed())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !m.connected.Load() {
			http.Error(w, "sin conexión al broker", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	return mux
}
