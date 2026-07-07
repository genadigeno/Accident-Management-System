-- Correlated incidents + the reports merged into them.

CREATE TABLE incidents (
    id                VARCHAR(36) PRIMARY KEY,
    accident_type     VARCHAR(20) NOT NULL,
    cell_lat          INT         NOT NULL,
    cell_lng          INT         NOT NULL,
    address           VARCHAR(255),
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    state             VARCHAR(10) NOT NULL DEFAULT 'OPEN',
    report_count      INT         NOT NULL,
    first_reported_at TIMESTAMPTZ NOT NULL,
    last_reported_at  TIMESTAMPTZ NOT NULL
);

-- The merge-candidate lookup: open incidents of a type in a grid-cell neighbourhood.
CREATE INDEX idx_incidents_candidates
    ON incidents (state, accident_type, cell_lat, cell_lng, last_reported_at);

CREATE TABLE incident_reports (
    -- the report's cacheId: the primary key makes correlation idempotent per report
    report_id   VARCHAR(64) PRIMARY KEY,
    incident_id VARCHAR(36) NOT NULL REFERENCES incidents (id),
    reported_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_incident_reports_incident ON incident_reports (incident_id);
