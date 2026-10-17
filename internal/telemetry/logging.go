package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
)

// Etapas del esquema de logs (sección 4.5); es lo que lee `expctl trace`.
const (
	StageConsume  = "consume"
	StageCommit   = "commit"
	StagePublish  = "publish"
	StageApply    = "apply"
	StageCommand  = "command"
	StageReceived = "received"
)

// Resultados habituales del campo result.
const (
	ResultOK        = "ok"
	ResultDuplicate = "duplicate"
	ResultStale     = "stale"
	ResultError     = "error"
)

// NewLogger crea un logger JSON por línea con ts (RFC3339Nano UTC) y service.
func NewLogger(service string, w io.Writer, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 {
				switch a.Key {
				case slog.TimeKey:
					a.Key = "ts"
					a.Value = slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339Nano))
				case slog.LevelKey:
					a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
				}
			}
			return a
		},
	})
	return slog.New(h).With("service", service)
}

// SetupLogger crea el logger del servicio (nivel por LOG_LEVEL, por defecto info),
// lo instala como slog.Default y lo devuelve.
func SetupLogger(service string) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}
	l := NewLogger(service, os.Stdout, level)
	slog.SetDefault(l)
	return l
}

// LogStage emite una línea de log de una etapa del ciclo de vida de un evento con
// los campos eventId, traceId, stage y result. args son atributos extra (clave, valor).
func LogStage(l *slog.Logger, stage string, env events.Envelope, result string, args ...any) {
	level := slog.LevelInfo
	if result == ResultError {
		level = slog.LevelError
	}
	base := []any{"eventId", env.EventID, "traceId", env.TraceID(), "stage", stage, "result", result}
	l.Log(context.Background(), level, stage, append(base, args...)...)
}
