-- Agregado Position: el último estado declarado de cada medidor. version = seq/version del fabricante.
CREATE TABLE positions (
    meter_id    text             PRIMARY KEY,
    side        text             NOT NULL CHECK (side IN ('SURPLUS', 'DEFICIT')),
    kwh         double precision NOT NULL CHECK (kwh >= 0),
    valid_until timestamptz      NOT NULL,
    zone        text             NOT NULL,
    version     bigint           NOT NULL CHECK (version > 0),
    -- clock_timestamp(): now() devolvería el inicio de la transacción.
    updated_at  timestamptz      NOT NULL DEFAULT clock_timestamp()
);
