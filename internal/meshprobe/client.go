package meshprobe

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const violationMarker = "Permissions Violation"

// Client envuelve la conexión NATS y recoge las violaciones de permisos,
// que el servidor notifica de forma asíncrona.
type Client struct {
	nc *nats.Conn
	js jetstream.JetStream

	mu         sync.Mutex
	violations []string
}

// Connect abre la conexión autenticada y el contexto JetStream.
func Connect(c Conn) (*Client, error) {
	cl := &Client{}
	nc, err := nats.Connect(c.URL,
		nats.UserInfo(c.User, c.Password),
		nats.Name("mesh-probe-"+c.User),
		nats.Timeout(5*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if strings.Contains(err.Error(), violationMarker) {
				cl.mu.Lock()
				cl.violations = append(cl.violations, err.Error())
				cl.mu.Unlock()
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("conectar a %s como %s: %w", c.URL, c.User, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("crear contexto JetStream: %w", err)
	}
	cl.nc, cl.js = nc, js
	return cl, nil
}

// Close vacía y cierra la conexión.
func (c *Client) Close() {
	_ = c.nc.Drain()
}

// Violations devuelve los mensajes de violación de permisos vistos hasta ahora.
func (c *Client) Violations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.violations...)
}

func (c *Client) violationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.violations)
}

// waitViolation espera hasta d a que el contador supere n (el servidor avisa de forma asíncrona).
func (c *Client) waitViolation(ctx context.Context, n int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if c.violationCount() > n {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
	return c.violationCount() > n
}
