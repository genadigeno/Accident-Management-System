-- Business identity of the accident (UUID assigned at ingestion). Replayed records (e.g.
-- re-published from the DLT) arrive with NEW Kafka coordinates, so offset-based idempotency
-- cannot catch them — this column deduplicates by identity and lets the same accident be
-- correlated across the responder services. NULL is allowed for legacy rows; the unique
-- index ignores NULLs.
ALTER TABLE emergency_accidents
    ADD COLUMN cache_id VARCHAR(64);

CREATE UNIQUE INDEX uq_emergency_accidents_cache_id
    ON emergency_accidents (cache_id);
