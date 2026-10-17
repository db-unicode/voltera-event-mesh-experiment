package amqpprobe

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// message es el cuerpo de cada mensaje de prueba.
type message struct {
	EventID string    `json:"eventId"`
	RunID   string    `json:"runId"`
	Seq     int       `json:"seq"`
	SentAt  time.Time `json:"sentAt"`
}

// newRunID devuelve un identificador corto de la ejecución (8 hex).
func newRunID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "00000000"
	}
	return hex.EncodeToString(b)
}

// buildBody serializa el mensaje número seq de la ejecución runID.
func buildBody(runID string, seq int, now time.Time) ([]byte, error) {
	return json.Marshal(message{
		EventID: fmt.Sprintf("%s-%d", runID, seq),
		RunID:   runID,
		Seq:     seq,
		SentAt:  now.UTC(),
	})
}

// exchangeFor devuelve el exchange por defecto de una cola: cada cola tiene un exchange direct
// `<cola>.x` enlazado con routing key = nombre de la cola. El permiso `write` se concede sobre ese
// exchange, no sobre amq.default, que permitiría publicar en cualquier cola.
func exchangeFor(queue string) string { return queue + ".x" }

// resolveTarget decide exchange y routing key. exchangeSet indica si el usuario pasó --exchange
// (incluso vacío, que significa el exchange por defecto).
func resolveTarget(exchange string, exchangeSet bool, queue string) (string, string, error) {
	if queue == "" {
		return "", "", errors.New("falta --queue (routing key)")
	}
	if !exchangeSet {
		exchange = exchangeFor(queue)
	}
	return exchange, queue, nil
}
