package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/devicecmd"
)

// HTTPSender envía el comando firmado a device-sim: POST <baseURL>/commands con el
// cuerpo JSON y la firma HMAC-SHA256 en X-Signature. 2xx ⇒ aplicado.
type HTTPSender struct {
	BaseURL string
	Key     []byte
	Client  *http.Client
}

// Send implementa Sender.
func (s HTTPSender) Send(ctx context.Context, cmd devicecmd.Command) error {
	body, sig, err := devicecmd.Encode(s.Key, cmd)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.BaseURL, "/")+"/commands", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(devicecmd.SignatureHeader, sig)
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("device unreachable: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("device responded %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
