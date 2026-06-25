-- Initial schema for the Statistics Service.
-- Windowed aggregates keyed by (window_end, window_start, accident type).

CREATE TABLE statistical_models (
    window_end   TIMESTAMP(6) NOT NULL,
    window_start TIMESTAMP(6) NOT NULL,
    type         VARCHAR(255) NOT NULL,
    count        BIGINT       NOT NULL,
    CONSTRAINT pk_statistical_models PRIMARY KEY (window_end, window_start, type)
);
