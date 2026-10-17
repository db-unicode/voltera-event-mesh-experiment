-- Tablas del spike S6: filas de negocio y un sumidero SIN unique para contar duplicados.
CREATE TABLE business (
    id         integer          PRIMARY KEY,
    meter_id   text             NOT NULL,
    kwh        double precision NOT NULL,
    created_at timestamptz      NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sink (
    event_id    uuid        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
