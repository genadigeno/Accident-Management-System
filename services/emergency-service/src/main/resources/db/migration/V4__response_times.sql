-- Response-time tracking: one row per dispatch, filled in as unit.status.events arrive.
-- dispatch_id is the natural primary key, so replayed/redelivered events are idempotent upserts.

CREATE TABLE response_times (
    dispatch_id      VARCHAR(36) PRIMARY KEY,
    incident_id      VARCHAR(64) NOT NULL,
    unit_id          VARCHAR(16),
    unit_type        VARCHAR(20) NOT NULL,
    dispatched_at    TIMESTAMPTZ,
    en_route_at      TIMESTAMPTZ,
    on_scene_at      TIMESTAMPTZ,
    cleared_at       TIMESTAMPTZ,
    -- dispatched_at -> on_scene_at, computed when the unit arrives
    response_seconds BIGINT,
    sla_breached     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_response_times_incident ON response_times (incident_id);
CREATE INDEX idx_response_times_breached ON response_times (sla_breached) WHERE sla_breached;
