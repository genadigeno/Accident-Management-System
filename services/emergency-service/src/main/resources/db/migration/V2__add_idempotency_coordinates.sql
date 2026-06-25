-- Idempotency: store the Kafka coordinates of each consumed record and enforce
-- uniqueness so re-delivered records cannot create duplicate rows.

ALTER TABLE emergency_accidents
    ADD COLUMN kafka_topic     VARCHAR(255) NOT NULL,
    ADD COLUMN kafka_partition INTEGER      NOT NULL,
    ADD COLUMN kafka_offset    BIGINT       NOT NULL;

CREATE UNIQUE INDEX uq_emergency_accidents_coordinates
    ON emergency_accidents (kafka_topic, kafka_partition, kafka_offset);
