package kafkaprobe

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// newMetricsHandler sirve /metrics (registro propio) y /healthz.
func newMetricsHandler(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

func newRegistry(consumed prometheus.Counter) (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	for _, c := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		consumed,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register collector: %w", err)
		}
	}
	return reg, nil
}

// serve levanta el servidor HTTP y lo apaga cuando ctx se cancela.
// Devuelve una función que espera el cierre y entrega el error del servidor.
func serve(ctx context.Context, addr string, h http.Handler) func() error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- err
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	return func() error { return <-errc }
}
