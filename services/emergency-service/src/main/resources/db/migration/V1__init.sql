-- Initial schema for the Emergency Service.

CREATE SEQUENCE emergency_accidents_id_seq INCREMENT BY 50 START WITH 1;

CREATE TABLE emergency_accidents (
    id            BIGINT       NOT NULL,
    address       VARCHAR(255),
    latitude      VARCHAR(255),
    longitude     VARCHAR(255),
    description   VARCHAR(255),
    accident_date DATE,
    CONSTRAINT pk_emergency_accidents PRIMARY KEY (id)
);
