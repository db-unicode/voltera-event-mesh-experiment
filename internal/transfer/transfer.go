// Package transfer es energy-transfer: consume EnergyMatched de Kafka y ordena la
// transferencia al dispositivo con un comando firmado, una sola vez por matchId.
//
// Política ante fallos del dispositivo (fail-safe): el comando se envía como mucho una
// vez. Si el envío falla, la transferencia queda FAILED y se registra TransferFailed;
// no hay reintento ciego porque un comando físico repetido puede transferir energía de
// más. Tampoco se reenvía un comando de una transferencia PENDING que quedó huérfana
// (crash entre registrar y enviar): la decisión de reintentar es de un operador, que
// los ve con GET /debug/state (transfers.PENDING) o en transfer-db.
package transfer

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/devicecmd"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/inbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations es la fuente de migraciones de transfer-db (junto con inbox.Migrations()).
func Migrations() migrate.Source {
	return migrate.Source{Name: "transfer", FS: migrationsFS, Dir: "migrations"}
}

// Nombres de estado de una transferencia.
const (
	StatusPending = "PENDING"
	StatusSent    = "SENT"
	StatusFailed  = "FAILED"
)

// ConsumerName es la clave de inbox de este consumidor.
const ConsumerName = "energy-transfer"

// FPBeforeAck: crash tras aplicar el efecto y antes de confirmar el offset de Kafka (E5).
const FPBeforeAck = "before_ack"

// Sender entrega el comando al dispositivo; error ⇒ no se aplicó (o no se sabe).
type Sender interface {
	Send(ctx context.Context, cmd devicecmd.Command) error
}

// Handler procesa un registro del topic energy-matched.
type Handler struct {
	Pool    *pgxpool.Pool
	Sender  Sender
	Svc     *telemetry.Service
	Metrics *telemetry.DomainMetrics
	FP      *failpoint.Registry
	Log     *slog.Logger
	Timeout time.Duration // acota el envío del comando (por defecto 5 s)
	Now     func() time.Time
}

// Handle implementa kafkax.Handler. Devuelve error solo ante fallos que conviene
// reintentar (base de datos); los fallos del dispositivo se resuelven aquí (FAILED).
func (h *Handler) Handle(ctx context.Context, rec kafkax.Record) error {
	now := time.Now
	if h.Now != nil {
		now = h.Now
	}
	env, err := events.Unmarshal(rec.Value)
	if err == nil && env.Type != events.TypeEnergyMatched {
		err = fmt.Errorf("unexpected event type %q", env.Type)
	}
	var m events.EnergyMatched
	if err == nil {
		m, err = events.DecodePayload[events.EnergyMatched](env)
	}
	if err == nil && m.MatchID == "" {
		err = errors.New("empty matchId")
	}
	if err != nil {
		// mensaje venenoso: reintentar no lo arregla y bloquearía la partición
		h.Log.Error("poison message skipped", "stage", "consume", "result", telemetry.ResultError,
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "error", err)
		return nil
	}
	if env.Traceparent == "" {
		env.Traceparent = rec.Headers[kafkax.HeaderTraceparent]
	}
	telemetry.LogStage(h.Log, telemetry.StageConsume, env, telemetry.ResultOK,
		"matchId", m.MatchID, "partition", rec.Partition, "offset", rec.Offset)

	fresh, err := h.register(ctx, env, m)
	if err != nil {
		return err // se reintenta el mismo mensaje
	}
	if !fresh {
		h.Metrics.TransferCommands.WithLabelValues("duplicate").Inc()
		h.Svc.Consumed(env, rec.Topic, telemetry.ResultDuplicate)
		telemetry.LogStage(h.Log, telemetry.StageApply, env, telemetry.ResultDuplicate, "matchId", m.MatchID)
		return nil
	}

	status, sendErr := h.send(ctx, env, m, now())
	if err := h.finish(ctx, m.MatchID, status, sendErr); err != nil {
		return err
	}
	h.Svc.Consumed(env, rec.Topic, telemetry.ResultOK)
	h.Svc.Metrics.ObserveLag(env, now())
	if h.FP != nil {
		h.FP.Hit(ctx, FPBeforeAck) // crash aquí: efecto aplicado, offset sin confirmar
	}
	return nil
}

// register inserta la marca de inbox y la transferencia PENDING en una transacción.
func (h *Handler) register(ctx context.Context, env events.Envelope, m events.EnergyMatched) (bool, error) {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("transfer: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	fresh, err := inbox.TryMark(ctx, tx, ConsumerName, m.MatchID)
	if err != nil {
		return false, err
	}
	if !fresh {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO transfers (match_id, event_id, seller_meter_id, buyer_meter_id, kwh, status)
		VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
		m.MatchID, env.EventID, m.SellerMeterID, m.BuyerMeterID, m.KWh); err != nil {
		return false, fmt.Errorf("transfer: insert %s: %w", m.MatchID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("transfer: commit: %w", err)
	}
	return true, nil
}

// send envía el comando una sola vez y devuelve el estado resultante.
func (h *Handler) send(ctx context.Context, env events.Envelope, m events.EnergyMatched, now time.Time) (string, error) {
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := devicecmd.Command{MatchID: m.MatchID, Seller: m.SellerMeterID, Buyer: m.BuyerMeterID, KWh: m.KWh, IssuedAt: now.UTC()}
	if err := h.Sender.Send(cctx, cmd); err != nil {
		h.Metrics.TransferCommands.WithLabelValues("failed").Inc()
		// TransferFailed: sin reintento ciego (ver la política en la doc del paquete)
		telemetry.LogStage(h.Log, telemetry.StageCommand, env, telemetry.ResultError,
			"event", "TransferFailed", "matchId", m.MatchID, "error", err)
		return StatusFailed, err
	}
	h.Metrics.TransferCommands.WithLabelValues("sent").Inc()
	telemetry.LogStage(h.Log, telemetry.StageCommand, env, telemetry.ResultOK, "matchId", m.MatchID, "kwh", m.KWh)
	return StatusSent, nil
}

func (h *Handler) finish(ctx context.Context, matchID, status string, sendErr error) error {
	var errText *string
	if sendErr != nil {
		s := sendErr.Error()
		errText = &s
	}
	if _, err := h.Pool.Exec(ctx,
		`UPDATE transfers SET status = $2, error = $3, updated_at = clock_timestamp() WHERE match_id = $1`,
		matchID, status, errText); err != nil {
		return fmt.Errorf("transfer: update %s: %w", matchID, err)
	}
	return nil
}

// StatusCounts devuelve cuántas transferencias hay por estado (/debug/state).
func StatusCounts(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `SELECT status, count(*) FROM transfers GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{StatusPending: 0, StatusSent: 0, StatusFailed: 0}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}
