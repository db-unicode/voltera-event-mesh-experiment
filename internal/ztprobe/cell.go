// Package ztprobe prueba, celda por celda, qué puede hacer una identidad en RabbitMQ, Kafka y NATS.
// Una celda es un intento (conectar, publicar, consumir o suscribir) que termina en "allowed" (el
// broker lo completó) o "denied" (cualquier rechazo: credencial, permiso del broker, política L4 de
// Istio o falta de mTLS). El campo Class dice qué capa lo rechazó, a título informativo.
package ztprobe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Brokers y operaciones soportadas.
const (
	BrokerRabbit = "rabbitmq"
	BrokerKafka  = "kafka"
	BrokerNATS   = "nats"

	OpPublish      = "publish"       // rabbitmq: vhost:cola; kafka: topic; nats: subject
	OpConsume      = "consume"       // rabbitmq: vhost:cola; kafka: topic
	OpSubscribe    = "subscribe"     // nats: subject
	OpConnect      = "connect"       // nats: (sin target)
	OpConsumerInfo = "consumer-info" // nats: STREAM/durable (JetStream API, no consume mensajes)
)

// Resultados.
const (
	Allowed = "allowed"
	Denied  = "denied"
)

// Clases de rechazo (heurística sobre el texto del error).
const (
	ClassAuthn   = "authn"   // credencial inválida o ausente
	ClassACL     = "acl"     // permiso del broker (RabbitMQ 403, NATS Permissions Violation)
	ClassNetwork = "network" // conexión cerrada, reseteada o sin respuesta: política L4 o falta de mTLS
	ClassOther   = "other"
)

// Cell es un intento. Target depende del broker (ver las constantes Op*).
type Cell struct {
	ID, Broker, Op, Target string
}

// Result es lo observado de una celda.
type Result struct {
	ID      string `json:"id"`
	Broker  string `json:"broker"`
	Op      string `json:"op"`
	Target  string `json:"target"`
	Outcome string `json:"outcome"`
	Class   string `json:"class,omitempty"`
	Error   string `json:"error,omitempty"`
	Ms      int64  `json:"ms"`
}

var validOps = map[string][]string{
	BrokerRabbit: {OpPublish, OpConsume},
	BrokerKafka:  {OpPublish, OpConsume},
	BrokerNATS:   {OpPublish, OpSubscribe, OpConnect, OpConsumerInfo},
}

// ParseCells lee CSV sin cabecera: id,broker,op,target (target puede quedar vacío en connect).
func ParseCells(r io.Reader) ([]Cell, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.Comment = '#'
	cr.TrimLeadingSpace = true
	var cells []Cell
	seen := map[string]bool{}
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) < 3 || len(rec) > 4 {
			return nil, fmt.Errorf("celda %q: se esperan id,broker,op[,target]", strings.Join(rec, ","))
		}
		c := Cell{ID: rec[0], Broker: rec[1], Op: rec[2]}
		if len(rec) == 4 {
			c.Target = rec[3]
		}
		ops, ok := validOps[c.Broker]
		if !ok {
			return nil, fmt.Errorf("celda %s: broker desconocido %q", c.ID, c.Broker)
		}
		if !contains(ops, c.Op) {
			return nil, fmt.Errorf("celda %s: operación %q no válida para %s", c.ID, c.Op, c.Broker)
		}
		if c.ID == "" || seen[c.ID] {
			return nil, fmt.Errorf("celda con id vacío o repetido: %q", c.ID)
		}
		if c.Target == "" && c.Op != OpConnect {
			return nil, fmt.Errorf("celda %s: falta target", c.ID)
		}
		seen[c.ID] = true
		cells = append(cells, c)
	}
	return cells, nil
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// Classify clasifica un error de cliente por su texto. Los brokers no dan códigos estables entre
// capas, así que es una heurística: la decisión permitido/rechazado no depende de ella.
func Classify(err error) string {
	if err == nil {
		return ""
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "login was refused"), strings.Contains(s, "authorization violation"),
		strings.Contains(s, "authentication"), strings.Contains(s, "sasl"):
		return ClassAuthn
	case strings.Contains(s, "access_refused"), strings.Contains(s, "permissions violation"),
		strings.Contains(s, "not_allowed"), strings.Contains(s, "authorization"):
		return ClassACL
	case strings.Contains(s, "eof"), strings.Contains(s, "reset"), strings.Contains(s, "refused"),
		strings.Contains(s, "deadline"), strings.Contains(s, "timeout"), strings.Contains(s, "timed out"),
		strings.Contains(s, "no responders"), strings.Contains(s, "no servers"), strings.Contains(s, "broken pipe"), strings.Contains(s, "closed"):
		return ClassNetwork
	}
	return ClassOther
}

func since(t time.Time) int64 { return time.Since(t).Milliseconds() }
