// Package meshprobe implementa el cliente de prueba del event mesh NATS:
// publica con ACK durable de JetStream y consume con un durable pull consumer.
package meshprobe

import (
	"errors"
	"fmt"
	"strings"
)

// Conn son los datos de conexión; vienen de env (Secret en el cluster).
type Conn struct {
	URL      string
	User     string
	Password string
}

// ConnFromEnv lee NATS_URL, NATS_USER y NATS_PASSWORD y falla si falta alguna.
func ConnFromEnv(getenv func(string) string) (Conn, error) {
	c := Conn{
		URL:      strings.TrimSpace(getenv("NATS_URL")),
		User:     getenv("NATS_USER"),
		Password: getenv("NATS_PASSWORD"),
	}
	var missing []string
	if c.URL == "" {
		missing = append(missing, "NATS_URL")
	}
	if c.User == "" {
		missing = append(missing, "NATS_USER")
	}
	if c.Password == "" {
		missing = append(missing, "NATS_PASSWORD")
	}
	if len(missing) > 0 {
		return Conn{}, fmt.Errorf("faltan variables de entorno: %s", strings.Join(missing, ", "))
	}
	if !strings.HasPrefix(c.URL, "nats://") && !strings.HasPrefix(c.URL, "tls://") {
		return Conn{}, errors.New("NATS_URL debe empezar con nats:// o tls://")
	}
	return c, nil
}
