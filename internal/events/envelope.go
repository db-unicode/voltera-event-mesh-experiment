// Package events define el sobre común de los eventos (compatible con CloudEvents)
// y los payloads de estado completo de Voltera. No depende de ningún broker.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Tipos de evento (campo type del sobre).
const (
	TypePositionDeclared = "PositionDeclared"
	TypeEnergyMatched    = "EnergyMatched"
	TypePriceUpdated     = "PriceUpdated"
)

// CurrentSchemaVersion es la versión de esquema que emiten los constructores.
const CurrentSchemaVersion = 1

// Envelope es el sobre JSON de todos los eventos. Los payloads llevan el estado
// completo (transferencia de estado): el consumidor no lee la base del emisor.
type Envelope struct {
	EventID       string `json:"eventId"`
	Type          string `json:"type"`
	SchemaVersion int    `json:"schemaVersion"`
	Region        string `json:"region"`
	AggregateID   string `json:"aggregateId"`
	Version       int64  `json:"version"`
	// OccurredAt lo fija el medidor o el motor de matching.
	OccurredAt time.Time `json:"occurredAt"`
	// CommittedAt es outbox.created_at; lo rellena el relay al leer la fila.
	CommittedAt time.Time `json:"committedAt,omitzero"`
	// EmittedAt lo fija el relay justo antes de publicar (se reescribe en cada reintento).
	EmittedAt   time.Time       `json:"emittedAt,omitzero"`
	Traceparent string          `json:"traceparent,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

// New crea un sobre con eventId UUIDv7 y el payload serializado. occurredAt se
// normaliza a UTC. Si traceparent es "", se omite del JSON.
func New(eventType, region, aggregateID string, version int64, occurredAt time.Time, payload any) (Envelope, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, fmt.Errorf("events: generate uuidv7: %w", err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("events: marshal payload: %w", err)
	}
	e := Envelope{
		EventID:       id.String(),
		Type:          eventType,
		SchemaVersion: CurrentSchemaVersion,
		Region:        region,
		AggregateID:   aggregateID,
		Version:       version,
		OccurredAt:    occurredAt.UTC(),
		Payload:       raw,
	}
	return e, e.Validate()
}

// NewPositionDeclared crea el evento de declaración; aggregateId = meterId.
func NewPositionDeclared(region string, version int64, occurredAt time.Time, p PositionDeclared) (Envelope, error) {
	return New(TypePositionDeclared, region, p.MeterID, version, occurredAt, p)
}

// NewPriceUpdated crea el evento de precio; aggregateId = región del precio.
func NewPriceUpdated(region string, version int64, occurredAt time.Time, p PriceUpdated) (Envelope, error) {
	return New(TypePriceUpdated, region, p.Region, version, occurredAt, p)
}

// NewEnergyMatched crea el evento de match; aggregateId = matchId.
func NewEnergyMatched(region string, version int64, occurredAt time.Time, p EnergyMatched) (Envelope, error) {
	return New(TypeEnergyMatched, region, p.MatchID, version, occurredAt, p)
}

// WithTraceparent devuelve una copia con el traceparent W3C fijado.
func (e Envelope) WithTraceparent(tp string) Envelope {
	e.Traceparent = tp
	return e
}

// TraceID extrae el trace-id de un traceparent W3C ("" si no es válido).
func (e Envelope) TraceID() string { return TraceID(e.Traceparent) }

// TraceID extrae el trace-id de un traceparent "00-<traceId>-<spanId>-<flags>".
func TraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

// Validate comprueba los campos obligatorios del sobre.
func (e Envelope) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("events: invalid eventId %q: %w", e.EventID, err)
	}
	switch {
	case e.Type == "":
		return errors.New("events: empty type")
	case e.SchemaVersion < 1:
		return fmt.Errorf("events: invalid schemaVersion %d", e.SchemaVersion)
	case e.AggregateID == "":
		return errors.New("events: empty aggregateId")
	case e.OccurredAt.IsZero():
		return errors.New("events: zero occurredAt")
	case len(e.Payload) == 0:
		return errors.New("events: empty payload")
	}
	return nil
}

// Marshal serializa el sobre a JSON tras validarlo.
func Marshal(e Envelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// Unmarshal parsea y valida un sobre JSON.
func Unmarshal(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, fmt.Errorf("events: unmarshal envelope: %w", err)
	}
	return e, e.Validate()
}

// DecodePayload deserializa el payload del sobre en T.
func DecodePayload[T any](e Envelope) (T, error) {
	var p T
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return p, fmt.Errorf("events: decode %s payload: %w", e.Type, err)
	}
	return p, nil
}
