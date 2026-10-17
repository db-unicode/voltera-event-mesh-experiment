package projection

import (
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/envcfg"
)

// Config del market-projector. No hay DSN de Postgres: el proyector no lee positions-db.
type Config struct {
	Region        string
	AMQP          amqpx.Config
	RedisAddr     string
	RedisPassword string
	PositionTTL   time.Duration
	PriceTTL      time.Duration
	Prefetch      int
	OpsAddr       string
}

// ConfigFromEnv lee y valida el entorno: REGION (r1|r2), AMQP_*, REDIS_ADDR
// (market-view.voltera-<región>:6379), REDIS_PASSWORD, VIEW_TTL_S (150), PRICE_TTL_S (300),
// PREFETCH (50) y OPS_ADDR (:9090).
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	r := envcfg.New(getenv)
	region := r.Region()
	c := Config{
		Region:        region,
		RedisAddr:     r.String("REDIS_ADDR", "market-view.voltera-"+region+":6379"),
		RedisPassword: r.Required("REDIS_PASSWORD"),
		PositionTTL:   time.Duration(r.Int("VIEW_TTL_S", 150, 1)) * time.Second,
		PriceTTL:      time.Duration(r.Int("PRICE_TTL_S", 300, 1)) * time.Second,
		Prefetch:      r.Int("PREFETCH", 50, 1),
		OpsAddr:       r.String("OPS_ADDR", ":9090"),
	}
	amqpCfg, amqpErr := amqpx.ConfigFromEnv(getenv)
	if err := envcfg.JoinErrs(r.Err(), amqpErr); err != nil {
		return Config{}, err
	}
	c.AMQP = amqpCfg
	return c, nil
}
