-- Initial schema for the Fire Rescue Service.

CREATE SEQUENCE fire_accidents_id_seq INCREMENT BY 50 START WITH 1;

CREATE TABLE fire_accidents (
    id            BIGINT       NOT NULL,
    address       VARCHAR(255),
    latitude      VARCHAR(255),
    longitude     VARCHAR(255),
    description   VARCHAR(255),
    accident_date DATE,
    CONSTRAINT pk_fire_accidents PRIMARY KEY (id)
);
