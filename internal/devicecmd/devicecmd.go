// Package devicecmd es el contrato del comando de transferencia entre energy-transfer
// y device-sim: cuerpo JSON y firma HMAC-SHA256 en el header X-Signature.
package devicecmd

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SignatureHeader es el header HTTP con la firma hexadecimal del cuerpo.
const SignatureHeader = "X-Signature"

// Command ordena al dispositivo ejecutar una transferencia. matchId es la clave de
// idempotencia del lado del dispositivo.
type Command struct {
	MatchID  string    `json:"matchId"`
	Seller   string    `json:"seller"`
	Buyer    string    `json:"buyer"`
	KWh      float64   `json:"kwh"`
	IssuedAt time.Time `json:"issuedAt"`
}

// Validate comprueba los campos obligatorios.
func (c Command) Validate() error {
	switch {
	case c.MatchID == "":
		return errors.New("devicecmd: empty matchId")
	case c.Seller == "" || c.Buyer == "":
		return errors.New("devicecmd: empty seller or buyer")
	case c.KWh <= 0:
		return fmt.Errorf("devicecmd: kwh must be positive, got %v", c.KWh)
	}
	return nil
}

// Encode serializa el comando y devuelve el cuerpo y su firma.
func Encode(key []byte, c Command) (body []byte, signature string, err error) {
	if err := c.Validate(); err != nil {
		return nil, "", err
	}
	body, err = json.Marshal(c)
	if err != nil {
		return nil, "", fmt.Errorf("devicecmd: marshal: %w", err)
	}
	return body, Sign(key, body), nil
}

// Sign calcula HMAC-SHA256(key, body) en hexadecimal.
func Sign(key, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// Verify comprueba la firma en tiempo constante.
func Verify(key, body []byte, signature string) bool {
	want, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return hmac.Equal(m.Sum(nil), want)
}
