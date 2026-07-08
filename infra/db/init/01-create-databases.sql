-- Per-service databases. Each AMS consumer owns its own database: they cannot share one
-- (their Flyway migration histories would collide). Mirrors k8s/postgres-init.configmap.yaml.
--
-- This runs automatically the FIRST time the ams-db container initialises an empty data
-- directory (Postgres runs everything in /docker-entrypoint-initdb.d). If you already have an
-- ams-db volume, recreate it first:  docker compose down -v && docker compose up -d
CREATE DATABASE ams_emergency;
CREATE DATABASE ams_lawenf;
CREATE DATABASE ams_firerescue;
CREATE DATABASE ams_statistics;
CREATE DATABASE ams_dispatch;
CREATE DATABASE ams_notification;
CREATE DATABASE ams_correlation;
