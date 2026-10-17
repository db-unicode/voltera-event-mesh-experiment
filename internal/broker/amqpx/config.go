// Package amqpx es el adaptador AMQP 0-9-1 (RabbitMQ) de los servicios: Publisher con
// publisher confirms y reconexión, y Consumer con ack manual, backoff y reconexión.
//
// No importa internal/outbox (arrastraría pgx a los binarios que solo consumen, como
// market-projector). El adaptador que implementa outbox.Publisher está en el
// subpaquete outboxpub.
package amqpx

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultHost = "rabbitmq.voltera-infra.svc"
	defaultPort = 5672

	dialTimeout = 10 * time.Second
	heartbeat   = 10 * time.Second
)

// Config son el destino y las credenciales de una conexión al broker. Cada servicio
// usa su propio usuario (permisos por regex en RabbitMQ) y el vhost de su región.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Vhost    string
	// ConnectionName aparece en la UI de management (propiedad connection_name).
	ConnectionName string
}

// ConfigFromEnv lee AMQP_HOST, AMQP_PORT, AMQP_USER, AMQP_PASSWORD y AMQP_VHOST.
// Si falta AMQP_VHOST usa REGION (el vhost de cada región se llama como ella).
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	c := Config{
		Host:     orDefault(getenv("AMQP_HOST"), defaultHost),
		Port:     defaultPort,
		User:     getenv("AMQP_USER"),
		Password: getenv("AMQP_PASSWORD"),
		Vhost:    orDefault(getenv("AMQP_VHOST"), getenv("REGION")),
	}
	var errs []error
	if p := getenv("AMQP_PORT"); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil || port < 1 || port > 65535 {
			errs = append(errs, fmt.Errorf("AMQP_PORT invalid: %q", p))
		} else {
			c.Port = port
		}
	}
	if c.User == "" {
		errs = append(errs, errors.New("AMQP_USER is required"))
	}
	if c.Password == "" {
		errs = append(errs, errors.New("AMQP_PASSWORD is required"))
	}
	if c.Vhost == "" {
		errs = append(errs, errors.New("AMQP_VHOST or REGION is required"))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ConfigFromURI arma la Config a partir de una URI amqp://user:pass@host:port/vhost.
func ConfigFromURI(uri string) (Config, error) {
	u, err := amqp.ParseURI(uri)
	if err != nil {
		return Config{}, fmt.Errorf("amqpx: parse uri: %w", err)
	}
	return Config{Host: u.Host, Port: u.Port, User: u.Username, Password: u.Password, Vhost: u.Vhost}, nil
}

// URI arma la URI AMQP escapando usuario, contraseña y vhost.
func (c Config) URI() string {
	return amqp.URI{Scheme: "amqp", Host: c.Host, Port: c.Port, Username: c.User, Password: c.Password, Vhost: c.Vhost}.String()
}

// dial abre una conexión nueva.
func (c Config) dial() (*amqp.Connection, error) {
	props := amqp.Table{}
	if c.ConnectionName != "" {
		props["connection_name"] = c.ConnectionName
	}
	conn, err := amqp.DialConfig(c.URI(), amqp.Config{
		Dial:       amqp.DefaultDial(dialTimeout),
		Heartbeat:  heartbeat,
		Properties: props,
	})
	if err != nil {
		return nil, fmt.Errorf("amqpx: dial %s:%d vhost %q: %w", c.Host, c.Port, c.Vhost, err)
	}
	return conn, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
