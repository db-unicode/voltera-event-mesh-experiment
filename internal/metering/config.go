package metering

import (
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/envcfg"
)

// Config del declaration-gateway (todo por variables de entorno).
type Config struct {
	Region      string
	DatabaseURL string
	AMQP        amqpx.Config
	Prefetch    int
	// Zones es el número de zonas del mercado: la ACL deriva con él la zona de los
	// medidores de acme. Debe coincidir con --zones de meter-sim.
	Zones int
	// ProcessingDelay simula el coste de validar/traducir cada declaración.
	ProcessingDelay time.Duration
	OpsAddr         string
}

// ConfigFromEnv lee y valida el entorno: REGION (r1|r2), DATABASE_URL, AMQP_*, PREFETCH
// (10), ZONES (4), PROCESSING_DELAY_MS (20) y OPS_ADDR (:9090).
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	r := envcfg.New(getenv)
	c := Config{
		Region:          r.Region(),
		DatabaseURL:     r.Required("DATABASE_URL"),
		Prefetch:        r.Int("PREFETCH", 10, 1),
		Zones:           r.Int("ZONES", 4, 1),
		ProcessingDelay: time.Duration(r.Int("PROCESSING_DELAY_MS", 20, 0)) * time.Millisecond,
		OpsAddr:         r.String("OPS_ADDR", ":9090"),
	}
	amqpCfg, amqpErr := amqpx.ConfigFromEnv(getenv)
	if err := r.Err(); err != nil || amqpErr != nil {
		return Config{}, envcfg.JoinErrs(err, amqpErr)
	}
	amqpCfg.ConnectionName = ServiceName
	c.AMQP = amqpCfg
	return c, nil
}
