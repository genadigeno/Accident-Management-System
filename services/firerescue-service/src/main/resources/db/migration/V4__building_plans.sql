-- Real (imported) building plans. address_key is the normalized lookup key; the DB-backed
-- provider falls back to the deterministic stub for addresses that are not stored here.

CREATE TABLE building_plans (
    id                  BIGSERIAL PRIMARY KEY,
    address_key         VARCHAR(255) NOT NULL,
    address             VARCHAR(255) NOT NULL,
    floors              INT          NOT NULL,
    fire_escape_routes  VARCHAR(4000),
    gas_line_locations  VARCHAR(4000),
    CONSTRAINT uq_building_plans_address_key UNIQUE (address_key)
);
