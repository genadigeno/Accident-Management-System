-- Notification history. The unique dedup_key makes alert consumption idempotent:
-- redeliveries, DLT replays and full-topic replays never notify twice.

CREATE TABLE notifications (
    id           BIGSERIAL PRIMARY KEY,
    dedup_key    VARCHAR(200) NOT NULL,
    source       VARCHAR(20)  NOT NULL,
    severity     VARCHAR(10)  NOT NULL,
    title        VARCHAR(200) NOT NULL,
    message      VARCHAR(1000),
    incident_id  VARCHAR(64),
    channels     VARCHAR(200),
    rate_limited BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uq_notifications_dedup_key UNIQUE (dedup_key)
);

CREATE INDEX idx_notifications_source ON notifications (source);
