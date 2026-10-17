-- Una transferencia por match. status: PENDING (registrada, comando sin enviar o sin
-- resultado), SENT (el dispositivo respondió 2xx) o FAILED (error al enviar: no se reintenta).
CREATE TABLE transfers (
    match_id        text             PRIMARY KEY,
    event_id        text             NOT NULL,
    seller_meter_id text             NOT NULL,
    buyer_meter_id  text             NOT NULL,
    kwh             double precision NOT NULL,
    status          text             NOT NULL CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    error           text,
    created_at      timestamptz      NOT NULL DEFAULT clock_timestamp(),
    updated_at      timestamptz      NOT NULL DEFAULT clock_timestamp()
);
