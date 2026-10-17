-- Un match por par oferente-demandante de una ventana. match_id es determinista
-- (events.MatchID): reprocesar la misma ventana con el mismo par no duplica.
CREATE TABLE matches (
    match_id        text             PRIMARY KEY,
    seller_meter_id text             NOT NULL,
    buyer_meter_id  text             NOT NULL,
    kwh             double precision NOT NULL,
    price_per_kwh   double precision NOT NULL,
    zone            text             NOT NULL,
    "window"        text             NOT NULL,
    created_at      timestamptz      NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX matches_window_idx ON matches ("window");

-- Una posición (meter_id, version) se empareja como máximo una vez: la PK impide que
-- dos ventanas (o dos réplicas) usen la misma versión de la posición.
CREATE TABLE matched_positions (
    meter_id text   NOT NULL,
    version  bigint NOT NULL,
    match_id text   NOT NULL REFERENCES matches (match_id),
    PRIMARY KEY (meter_id, version)
);
