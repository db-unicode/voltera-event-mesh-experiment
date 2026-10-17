-- Idempotencia de consumidor: una fila por (consumidor, evento) ya procesado.
CREATE TABLE inbox (
    consumer     text        NOT NULL,
    event_key    text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer, event_key)
);
