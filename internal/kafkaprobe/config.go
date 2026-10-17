// Package kafkaprobe implementa el cliente de prueba de Kafka usado en los spikes S1-S3.
package kafkaprobe

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

const (
	defaultBrokers     = "kafka.voltera-infra.svc:9092"
	defaultMetricsAddr = ":8080"
)

// Config se lee de variables de entorno y se valida al arrancar.
type Config struct {
	Brokers     []string
	MetricsAddr string
}

// LoadConfig lee KAFKA_BROKERS (lista separada por comas) y METRICS_ADDR.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{MetricsAddr: defaultMetricsAddr}

	raw := getenv("KAFKA_BROKERS")
	if raw == "" {
		raw = defaultBrokers
	}
	for _, b := range strings.Split(raw, ",") {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(b); err != nil {
			return Config{}, fmt.Errorf("KAFKA_BROKERS: invalid broker %q: %w", b, err)
		}
		cfg.Brokers = append(cfg.Brokers, b)
	}
	if len(cfg.Brokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS: no brokers")
	}

	if v := getenv("METRICS_ADDR"); v != "" {
		if _, _, err := net.SplitHostPort(v); err != nil {
			return Config{}, fmt.Errorf("METRICS_ADDR: invalid address %q: %w", v, err)
		}
		cfg.MetricsAddr = v
	}
	return cfg, nil
}
