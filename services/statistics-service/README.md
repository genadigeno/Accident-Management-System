# Statistics Service

Part of the [Accident Management System (AMS)](../../README.md). See the root README for the
full architecture, the plain-language overview, and end-to-end setup.

## What this service does

Consumes **aggregated statistics events** from Kafka (default topic `statistics.events`) and
persists them to PostgreSQL. The aggregation itself is performed upstream by the
[`accident-event-stream`](../accident-event-stream) router using a Kafka Streams **windowing**
operation, keyed by window start time, window end time, and `AccidentType`. Records that cannot
be processed are routed to a dead-letter topic (`statistics.events.dlt`) so nothing is silently lost.

## Features

### Real-time metrics API

Read-only REST endpoints over the stored windowed aggregates (default port `48089`):

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/stats/by-type` | Total event count per accident type, most frequent first |
| `GET /api/v1/stats/summary` | `{ totalEvents, mostCommonType, byType[] }` |
| `GET /api/v1/stats/recent?limit=50` | The most recent windowed aggregates (max 1000) |
| `GET /api/v1/stats/hourly?hours=24` | Events per hour per type ("accidents per hour") |
| `GET /api/v1/stats/daily?days=30` | Events per day per type |
| `GET /api/v1/stats/weekly?weeks=12` | Events per week per type |
| `GET /api/v1/stats/trend?period=day` | Period-over-period change — "crime rate increased by 20%" (`changePercent` is `null` without a baseline) |
| `GET /api/v1/stats/peak-hours` | Events per hour of day, busiest first — "peak accident hours" |
| `GET /api/v1/reports/daily.csv?days=30` | Downloadable CSV report for city officials |

```bash
curl http://localhost:48089/api/v1/stats/summary
# {"totalEvents":1234,"mostCommonType":"CAR_ACCIDENT","byType":[{"type":"CAR_ACCIDENT","total":540}, ...]}
```

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster, Schema Registry, and PostgreSQL — see the
[root README → Installation](../../README.md#-installation).

```bash
java -jar target/statistics-service.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `48089` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `MAIN_TOPIC_NAME` | `statistics.events` | Topic this service consumes |
| `DLT_TOPIC_NAME` | `statistics.events.dlt` | Dead-letter topic |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/accident_management_service` | JDBC URL |
| `POSTGRES_USER` | `test` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `SPRING_PROFILES_ACTIVE` | `dev` | Active profile (`dev` / `prod`) |

> The database schema is created and versioned by **Flyway** (`src/main/resources/db/migration`);
> both profiles run Hibernate in `validate` mode. For a fresh local DB this happens automatically.

## Tech stack

Java 17 · Spring Boot 3.4.x (Data JPA, Kafka Streams) · Apache Avro + Schema Registry · Lombok · PostgreSQL · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
