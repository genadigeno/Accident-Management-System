# Law Enforcement (Police) Service

Part of the [Accident Management System (AMS)](../../README.md). See the root README for the
full architecture, the plain-language overview, and end-to-end setup.

## What this service does

Consumes **police events** from Kafka (default topic `law-enforcement.events`), maps them, and
persists them to PostgreSQL. Records that cannot be processed are routed to a dead-letter
topic (`law-enforcement.events.dlt`) so nothing is silently lost.

## Features

### Automatic BOLO ("Be On the Lookout") alerts

Each incoming incident's description is scanned for threat keywords and classified:

| Level | Triggers (keywords) | Intent |
|-------|---------------------|--------|
| `CRITICAL` | gun, firearm, weapon, hostage, bomb, explosive, shooting | page SWAT / counter-terrorism |
| `HIGH` | stolen vehicle, armed robbery, kidnap | broadcast to patrol cars |
| `NONE` | (no match) | — |

The level is **persisted** on each record (`bolo_level`), a `WARN` **alert** is logged, and a
Micrometer counter `ams.bolo.alerts{level=…}` is incremented (visible in Prometheus/Grafana).

```bash
curl http://localhost:28089/api/v1/bolo      # 50 most recent HIGH + CRITICAL incidents
```

### Crime hotspots & forecast

Geographic analytics over the persisted incidents: the busiest ~1 km cells, and a lightweight
**EWMA forecast** of each hotspot's next-day crime count (an exponentially-weighted moving
average over the daily series — the honest baseline; a real model can replace it behind the
service).

```bash
curl "http://localhost:28089/api/v1/crime/hotspots?days=30&limit=20"
# [ { "latitude":41.70, "longitude":44.80, "total":8, "boloCount":3, "sampleAddress":"..." }, ... ]

curl "http://localhost:28089/api/v1/crime/forecast?days=30&limit=10&alpha=0.5"
# [ { "latitude":41.70, "longitude":44.80, "recentTotal":8, "dailyCounts":[...], "predictedNextDay":1.6 }, ... ]
```

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster, Schema Registry, and PostgreSQL — see the
[root README → Installation](../../README.md#-installation).

```bash
java -jar target/law-enforcement-service.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `28089` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `MAIN_TOPIC_NAME` | `law-enforcement.events` | Topic this service consumes |
| `DLT_TOPIC_NAME` | `law-enforcement.events.dlt` | Dead-letter topic |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/accident_management_service` | JDBC URL |
| `POSTGRES_USER` | `test` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `SPRING_PROFILES_ACTIVE` | `dev` | Active profile (`dev` / `prod`) |

> The database schema is created and versioned by **Flyway** (`src/main/resources/db/migration`);
> both profiles run Hibernate in `validate` mode. For a fresh local DB this happens automatically.

## Tech stack

Java 17 · Spring Boot 3.4.x (Data JPA, Kafka) · Apache Avro + Schema Registry · MapStruct · Lombok · PostgreSQL · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
