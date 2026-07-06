-- Initial schema for the Dispatch Service: the response-unit fleet and the dispatch log.

CREATE TABLE units (
    id        VARCHAR(16) PRIMARY KEY,
    type      VARCHAR(20) NOT NULL,
    state     VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE',
    latitude  DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE dispatches (
    id                 VARCHAR(36) PRIMARY KEY,
    cache_id           VARCHAR(64) NOT NULL,
    unit_type          VARCHAR(20) NOT NULL,
    unit_id            VARCHAR(16),
    status             VARCHAR(20) NOT NULL,
    address            VARCHAR(255),
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    created_at         TIMESTAMPTZ NOT NULL,
    dispatched_at      TIMESTAMPTZ,
    en_route_at        TIMESTAMPTZ,
    on_scene_at        TIMESTAMPTZ,
    cleared_at         TIMESTAMPTZ,
    next_transition_at TIMESTAMPTZ,
    -- Idempotency: one dispatch per (incident, unit type), however often the event is redelivered.
    CONSTRAINT uq_dispatches_incident_unit_type UNIQUE (cache_id, unit_type)
);

CREATE INDEX idx_dispatches_status ON dispatches (status);
CREATE INDEX idx_dispatches_next_transition
    ON dispatches (next_transition_at)
    WHERE next_transition_at IS NOT NULL;

-- Demo fleet, spread around the city.
INSERT INTO units (id, type, state, latitude, longitude) VALUES
    ('POL-1',  'POLICE_CAR',  'AVAILABLE', 41.700, 44.780),
    ('POL-2',  'POLICE_CAR',  'AVAILABLE', 41.720, 44.820),
    ('POL-3',  'POLICE_CAR',  'AVAILABLE', 41.680, 44.850),
    ('POL-4',  'POLICE_CAR',  'AVAILABLE', 41.740, 44.760),
    ('AMB-1',  'AMBULANCE',   'AVAILABLE', 41.710, 44.790),
    ('AMB-2',  'AMBULANCE',   'AVAILABLE', 41.690, 44.830),
    ('AMB-3',  'AMBULANCE',   'AVAILABLE', 41.730, 44.800),
    ('AMB-4',  'AMBULANCE',   'AVAILABLE', 41.670, 44.810),
    ('FIRE-1', 'FIRE_ENGINE', 'AVAILABLE', 41.705, 44.770),
    ('FIRE-2', 'FIRE_ENGINE', 'AVAILABLE', 41.725, 44.840),
    ('FIRE-3', 'FIRE_ENGINE', 'AVAILABLE', 41.685, 44.795),
    ('FIRE-4', 'FIRE_ENGINE', 'AVAILABLE', 41.745, 44.825);
