package meshprobe

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PublishOptions configura publish. Cada mensaje lleva Nats-Msg-Id = <MsgIDPrefix>-<n>.
type PublishOptions struct {
	Subject     string
	Count       int
	MsgIDPrefix string
	Repeat      int           // veces que se publica cada id (>1 para probar dedupe)
	AckTimeout  time.Duration // espera máxima del PubAck por intento
	// RetryTimeout reintenta cada mensaje hasta obtener ACK o agotar este tiempo (0 = sin reintentos).
	RetryTimeout time.Duration
}

// PublishResult resume una ejecución de publish.
type PublishResult struct {
	Subject       string   `json:"subject"`
	Attempted     int      `json:"attempted"`
	Acks          int      `json:"acks"`
	DuplicateAcks int      `json:"duplicate_acks"`
	Errors        int      `json:"errors"`
	Violations    int      `json:"violations"`
	ViolationMsgs []string `json:"violation_msgs,omitempty"`
	FirstError    string   `json:"first_error,omitempty"`
	Streams       []string `json:"streams,omitempty"`
	ElapsedMS     int64    `json:"elapsed_ms"`
	FirstAckMS    int64    `json:"first_ack_ms"` // -1 si no hubo ACK
	LastSeq       uint64   `json:"last_seq"`
	Retries       int      `json:"retries"`
}

// Publish publica con JetStream y cuenta PubAcks y errores. Un mensaje solo cuenta como
// confirmado si el stream devolvió PubAck: sin ACK no hay confirmación.
func (c *Client) Publish(ctx context.Context, o PublishOptions) PublishResult {
	res := PublishResult{Subject: o.Subject, FirstAckMS: -1}
	start := time.Now()
	streams := map[string]struct{}{}
	for rep := 0; rep < max(o.Repeat, 1); rep++ {
		for i := 1; i <= o.Count; i++ {
			if ctx.Err() != nil {
				break
			}
			id := fmt.Sprintf("%s-%d", o.MsgIDPrefix, i)
			res.Attempted++
			ack, retries, err := c.publishOne(ctx, o, id)
			res.Retries += retries
			if err != nil {
				res.Errors++
				if res.FirstError == "" {
					res.FirstError = err.Error()
				}
				continue
			}
			res.Acks++
			if ack.Duplicate {
				res.DuplicateAcks++
			}
			if res.FirstAckMS < 0 {
				res.FirstAckMS = time.Since(start).Milliseconds()
			}
			res.LastSeq = ack.Sequence
			streams[ack.Stream] = struct{}{}
		}
	}
	for s := range streams {
		res.Streams = append(res.Streams, s)
	}
	res.ElapsedMS = time.Since(start).Milliseconds()
	res.ViolationMsgs = c.Violations()
	res.Violations = len(res.ViolationMsgs)
	return res
}

func (c *Client) publishOne(ctx context.Context, o PublishOptions, id string) (*jetstream.PubAck, int, error) {
	deadline := time.Now().Add(o.RetryTimeout)
	backoff := 200 * time.Millisecond
	retries := 0
	for {
		msg := &nats.Msg{
			Subject: o.Subject,
			Header:  nats.Header{jetstream.MsgIDHeader: []string{id}},
			Data:    []byte(fmt.Sprintf(`{"matchId":%q}`, id)),
		}
		before := c.violationCount()
		attemptCtx, cancel := context.WithTimeout(ctx, o.AckTimeout)
		ack, err := c.js.PublishMsg(attemptCtx, msg, jetstream.WithRetryAttempts(0))
		cancel()
		if err == nil {
			return ack, retries, nil
		}
		// Una violación de permisos no se arregla reintentando.
		if c.waitViolation(ctx, before, 100*time.Millisecond) {
			return nil, retries, fmt.Errorf("%w (violación de permisos)", err)
		}
		if o.RetryTimeout <= 0 || ctx.Err() != nil || time.Now().Add(backoff).After(deadline) {
			return nil, retries, err
		}
		retries++
		select {
		case <-ctx.Done():
			return nil, retries, errors.Join(err, ctx.Err())
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Second)
	}
}
