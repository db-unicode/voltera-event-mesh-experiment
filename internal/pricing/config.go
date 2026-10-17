package pricing

import (
	"errors"
	"fmt"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/envcfg"
)

// Config del price-feed-simulator.
type Config struct {
	Region      string
	AMQP        amqpx.Config
	MinInterval time.Duration
	MaxInterval time.Duration
	Base        float64
	Jitter      float64
	Seed        int64
	OpsAddr     string
}

// ConfigFromEnv lee y valida el entorno: REGION (r1|r2), AMQP_*, PRICE_MIN_INTERVAL_S (5),
// PRICE_MAX_INTERVAL_S (15), PRICE_BASE (0.15), PRICE_JITTER (0.01), PRICE_SEED (1) y OPS_ADDR (:9090).
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	r := envcfg.New(getenv)
	c := Config{
		Region:      r.Region(),
		MinInterval: time.Duration(r.Float("PRICE_MIN_INTERVAL_S", 5, 0.01) * float64(time.Second)),
		MaxInterval: time.Duration(r.Float("PRICE_MAX_INTERVAL_S", 15, 0.01) * float64(time.Second)),
		Base:        r.Float("PRICE_BASE", 0.15, 0.0001),
		Jitter:      r.Float("PRICE_JITTER", 0.01, 0),
		Seed:        r.Int64("PRICE_SEED", 1),
		OpsAddr:     r.String("OPS_ADDR", ":9090"),
	}
	amqpCfg, amqpErr := amqpx.ConfigFromEnv(getenv)
	var rangeErr error
	if c.MaxInterval < c.MinInterval {
		rangeErr = fmt.Errorf("PRICE_MAX_INTERVAL_S (%s) must be >= PRICE_MIN_INTERVAL_S (%s)", c.MaxInterval, c.MinInterval)
	}
	if c.Jitter > 0 && c.Base-boundSteps*c.Jitter <= 0 {
		rangeErr = errors.Join(rangeErr, fmt.Errorf("PRICE_JITTER %v is too large for PRICE_BASE %v (the lower bound would be <= 0)", c.Jitter, c.Base))
	}
	if err := envcfg.JoinErrs(r.Err(), amqpErr, rangeErr); err != nil {
		return Config{}, err
	}
	amqpCfg.ConnectionName = ServiceName
	c.AMQP = amqpCfg
	return c, nil
}
