package matching

import "fmt"

// OutboxMode elige cómo sale el EnergyMatched tras confirmar el match.
type OutboxMode string

const (
	// ModeOutbox (por defecto): el evento se encola en la outbox en la misma transacción
	// que el match y lo publica el relay.
	ModeOutbox OutboxMode = "outbox"
	// ModeDirect es el control de E6: escritura dual ingenua. Se confirma el match SIN
	// outbox y luego se publica directo a Kafka; una caída entre ambos pasos pierde el evento.
	ModeDirect OutboxMode = "direct"
)

// ParseOutboxMode valida OUTBOX_MODE ("" equivale a outbox).
func ParseOutboxMode(s string) (OutboxMode, error) {
	switch OutboxMode(s) {
	case "", ModeOutbox:
		return ModeOutbox, nil
	case ModeDirect:
		return ModeDirect, nil
	}
	return "", fmt.Errorf("OUTBOX_MODE must be outbox or direct, got %q", s)
}
