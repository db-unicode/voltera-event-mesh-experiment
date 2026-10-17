// Package metrics contiene el generador del probe del spike S13: un servicio sintético que
// "consume" eventos a un ritmo fijo y expone las métricas comunes de telemetry.
package metrics

import (
	"context"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

// Channel es el canal ficticio de los eventos sintéticos.
const Channel = "probe"

// Generate consume un evento sintético cada period hasta que ctx se cancele.
// Cada 10.º es duplicado y cada 25.º es viejo (stale); el resto son ok.
func Generate(ctx context.Context, svc *telemetry.Service, period time.Duration) error {
	tick := time.NewTicker(period)
	defer tick.Stop()
	for i := 1; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		lag := time.Duration(10+i%90) * time.Millisecond
		env, err := events.NewPositionDeclared("r1", int64(i), time.Now().Add(-lag), events.PositionDeclared{
			MeterID: "m-probe", Side: events.SideSurplus, KWh: 1, Zone: "z1"})
		if err != nil {
			return err
		}
		result := telemetry.ResultOK
		switch {
		case i%25 == 0:
			result = telemetry.ResultStale
		case i%10 == 0:
			result = telemetry.ResultDuplicate
		}
		svc.Consumed(env, Channel, result)
		svc.Metrics.ProcessingSeconds.WithLabelValues(env.Type).Observe(lag.Seconds() / 10)
		svc.Metrics.ObserveLag(env, time.Now())
	}
}
