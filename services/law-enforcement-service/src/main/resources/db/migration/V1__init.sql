-- Initial schema for the Law Enforcement Service.

CREATE SEQUENCE law_enforcement_accidents_id_seq INCREMENT BY 50 START WITH 1;

CREATE TABLE law_enforcement_accidents (
    id            BIGINT       NOT NULL,
    address       VARCHAR(255),
    latitude      VARCHAR(255),
    longitude     VARCHAR(255),
    description   VARCHAR(255),
    accident_date DATE,
    CONSTRAINT pk_law_enforcement_accidents PRIMARY KEY (id)
);
