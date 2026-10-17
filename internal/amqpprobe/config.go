// Package amqpprobe es un cliente AMQP 0-9-1 de prueba para RabbitMQ: publica con confirms,
// consume con ack manual y corre como consumidor continuo con métricas.
package amqpprobe

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
)

// Config son las credenciales y el destino del broker, leídos de variables de entorno.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	// Vhost es el valor de AMQP_VHOST; el flag --vhost de cada subcomando lo sobrescribe.
	Vhost string
}

// LoadConfig lee AMQP_HOST, AMQP_PORT, AMQP_USER, AMQP_PASSWORD y AMQP_VHOST.
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{
		Host:     orDefault(getenv("AMQP_HOST"), defaultHost),
		Port:     defaultPort,
		User:     getenv("AMQP_USER"),
		Password: getenv("AMQP_PASSWORD"),
		Vhost:    getenv("AMQP_VHOST"),
	}
	if p := getenv("AMQP_PORT"); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("AMQP_PORT inválido: %q", p)
		}
		c.Port = port
	}
	var errs []error
	if c.User == "" {
		errs = append(errs, errors.New("falta AMQP_USER"))
	}
	if c.Password == "" {
		errs = append(errs, errors.New("falta AMQP_PASSWORD"))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return c, nil
}

// URI arma la URI AMQP escapando usuario, contraseña y vhost.
func (c Config) URI(vhost string) string {
	return amqp.URI{
		Scheme:   "amqp",
		Host:     c.Host,
		Port:     c.Port,
		Username: c.User,
		Password: c.Password,
		Vhost:    vhost,
	}.String()
}

// Dial abre una conexión al vhost indicado.
func (c Config) Dial(vhost, name string) (*amqp.Connection, error) {
	return amqp.DialConfig(c.URI(vhost), amqp.Config{
		Dial:       amqp.DefaultDial(10 * time.Second),
		Heartbeat:  10 * time.Second,
		Properties: amqp.Table{"connection_name": name},
	})
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
