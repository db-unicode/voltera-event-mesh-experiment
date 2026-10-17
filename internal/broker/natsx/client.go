// Package natsx son los adaptadores de NATS JetStream (nats.go) de los servicios: un
// Publisher que solo confirma con el PubAck del stream y un PullConsumer durable con
// ack explícito. La publicación sigue el ADR 0005 (el ACK del stream global habilita
// el commit del offset de Kafka).
package natsx

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const violationMarker = "Permissions Violation"

// Config son los datos de conexión (en el cluster vienen de un Secret).
type Config struct {
	URL      string
	User     string
	Password string
	Name     string // nombre de la conexión en el servidor
}

// Client envuelve la conexión y recoge las violaciones de permisos, que el servidor
// notifica de forma asíncrona.
type Client struct {
	nc *nats.Conn
	js jetstream.JetStream

	mu         sync.Mutex
	violations int
}

// Connect abre la conexión autenticada y el contexto JetStream. Reconecta sola.
func Connect(cfg Config) (*Client, error) {
	if cfg.URL == "" || cfg.User == "" || cfg.Password == "" {
		return nil, errors.New("natsx: url, user and password are required")
	}
	if !strings.HasPrefix(cfg.URL, "nats://") && !strings.HasPrefix(cfg.URL, "tls://") {
		return nil, errors.New("natsx: url must start with nats:// or tls://")
	}
	c := &Client{}
	nc, err := nats.Connect(cfg.URL,
		nats.UserInfo(cfg.User, cfg.Password),
		nats.Name(cfg.Name),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.RetryOnFailedConnect(true),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if strings.Contains(err.Error(), violationMarker) {
				c.mu.Lock()
				c.violations++
				c.mu.Unlock()
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("natsx: connect %s as %s: %w", cfg.URL, cfg.User, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsx: jetstream: %w", err)
	}
	c.nc, c.js = nc, js
	return c, nil
}

// Healthy devuelve error si la conexión no está establecida (readyz).
func (c *Client) Healthy() error {
	if s := c.nc.Status(); s != nats.CONNECTED {
		return fmt.Errorf("natsx: connection status %s", s)
	}
	return nil
}

// Violations devuelve cuántas violaciones de permisos notificó el servidor.
func (c *Client) Violations() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.violations
}

// Close vacía y cierra la conexión.
func (c *Client) Close() { _ = c.nc.Drain() }
