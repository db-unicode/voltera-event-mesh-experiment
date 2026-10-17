package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

func TestGenerateExposesConsumedMetrics(t *testing.T) {
	svc := telemetry.New(telemetry.Info{Service: "metrics-probe"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	require.NoError(t, Generate(ctx, svc, 5*time.Millisecond))

	w := httptest.NewRecorder()
	svc.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := w.Body.String()
	require.Contains(t, body, `events_consumed_total{channel="probe",event_type="PositionDeclared",result="ok",service="metrics-probe"}`)
	require.Contains(t, body, `events_consumed_total{channel="probe",event_type="PositionDeclared",result="duplicate",service="metrics-probe"}`)
	require.Contains(t, body, `event_e2e_lag_seconds_bucket`)
}
