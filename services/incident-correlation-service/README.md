# Incident Correlation Service

Part of the [Accident Management System (AMS)](../../README.md). Solves a real 911 problem:
**many people report the same accident** — without correlation every call looks like a new
incident. This service merges duplicate reports into ONE incident with a growing report count.

## What this service does

Consumes every raw report (`accident.events`) and correlates by three signals:

| Signal | Rule |
|--------|------|
| **Type** | same `AccidentType` |
| **Space** | same ~150 m grid cell, or one of its 8 neighbours |
| **Time** | the incident received a report within the last `CORRELATION_WINDOW_MINUTES` |

A report matching an open incident **merges** into it (`reportCount++`); otherwise a new
incident **opens**. An incident with no new reports for `CORRELATION_CLOSE_MINUTES` **closes**
automatically. Every change is published to **`incident.events`**
(Avro `IncidentEvent`: `OPENED / UPDATED / CLOSED`, keyed by incident id) after the DB
transaction commits.

Correlation is **idempotent per report** (the report's `cacheId` is the primary key), so Kafka
redeliveries and replays never inflate the count. Candidate selection runs under a pessimistic
lock so the listener and the auto-close scheduler can't race.

## API

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/incidents` | 50 most recently active incidents |
| `GET /api/v1/incidents/{id}` | One incident + ids of all merged reports |
| `GET /api/v1/incidents/nearby?lat=&lng=&type=` | The open incident a new report there would join — powers the [gateway](../citizen-report-gateway)'s duplicate hint |

Metrics: `ams.incidents.opened` / `merged` / `closed`, at `/actuator/prometheus`.

## Build & run

```bash
mvn clean package
java -jar target/incident-correlation-service.jar
```

Requires Kafka + Schema Registry + PostgreSQL — see the [root README](../../README.md#-installation).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8089` | HTTP port |
| `BOOTSTRAP_SERVERS` / `SCHEMA_REGISTRY_URL` | localhost | Kafka / Schema Registry |
| `SOURCE_TOPIC_NAME` | `accident.events` | Reports consumed |
| `INCIDENT_TOPIC_NAME` | `incident.events` | Lifecycle events produced |
| `DLT_TOPIC_NAME` | `correlation.events.dlt` | Dead-letter topic |
| `CORRELATION_WINDOW_MINUTES` | `10` | Merge window (since the incident's last report) |
| `CORRELATION_CLOSE_MINUTES` | `30` | Idle time before auto-close |
| `POSTGRES_URL` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | dev defaults | Database |
| `SPRING_PROFILES_ACTIVE` | `dev` | `dev` / `prod` |

## Tech stack

Java 17 · Spring Boot 3.4.x (Data JPA, Kafka) · Apache Avro + Schema Registry · Lombok · PostgreSQL · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
