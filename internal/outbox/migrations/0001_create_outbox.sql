-- Outbox transaccional: el evento se inserta en la misma transacción que el estado de negocio.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,
    aggregate_id text        NOT NULL,
    type         text        NOT NULL,
    topic        text        NOT NULL,
    key          text        NOT NULL,
    payload      jsonb       NOT NULL,
    -- clock_timestamp(): now() devolvería el inicio de la transacción.
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);

-- Solo las filas pendientes: el relay y las métricas no recorren el histórico.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
