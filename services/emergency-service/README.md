# Emergency Service

Part of the [Accident Management System (AMS)](../../README.md). See the root README for the
full architecture, the plain-language overview, and end-to-end setup.

## What this service does

Consumes **emergency events** from Kafka (default topic `emergency.events`), maps them, and
persists them to PostgreSQL. Records that cannot be processed are routed to a dead-letter
topic (`emergency.events.dlt`) so nothing is silently lost.

## Features

### Response-time & SLA tracking

Consumes **`unit.status.events`** from the [dispatch service](../dispatch-service) and records,
per dispatch, when the unit was dispatched, en route, on scene, and cleared. On arrival it
computes the **response time** (dispatched → on scene) and, when it exceeds the SLA
(`RESPONSE_SLA_SECONDS`, default 15 minutes), flags the row, logs a `WARN` alert, and increments
`ams.sla.breached{unitType=…}`. Every response is also recorded in the `ams.response.time` timer
(both visible in Prometheus/Grafana).

```bash
curl http://localhost:18089/api/v1/response-times           # 50 most recent responses
curl http://localhost:18089/api/v1/response-times/summary   # totals, avg, breaches — overall + per unit type
```

### Nearest-hospital lookup

Finds hospitals near an accident location so responders can be directed to the closest one.
Backed by the **free OpenStreetMap Overpass API** (no API key); results are sorted by
great-circle (Haversine) distance. The [`HospitalProvider`](src/main/java/ams/emergency/hospital/HospitalProvider.java)
interface lets a commercial provider (e.g. Google Places) be swapped in without touching the
service or controller.

```bash
curl "http://localhost:18089/api/v1/hospitals/nearby?lat=41.7151&lng=44.8271&radius=5000"
# [ { "name": "...", "latitude": 41.72, "longitude": 44.83, "distanceMeters": 1240 }, ... ]
```

`radius` is metres (100–50000, default 5000). Invalid coordinates return `400`; if the provider
is unreachable the endpoint degrades gracefully to `502`.

| Variable | Default | Description |
|----------|---------|-------------|
| `OVERPASS_URL` | `https://overpass-api.de/api/interpreter` | Overpass endpoint |
| `OVERPASS_CONNECT_TIMEOUT_MS` / `OVERPASS_READ_TIMEOUT_MS` | `3000` / `30000` | HTTP timeouts |

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster, Schema Registry, and PostgreSQL — see the
[root README → Installation](../../README.md#-installation).

```bash
java -jar target/emergency-service.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `18089` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `MAIN_TOPIC_NAME` | `emergency.events` | Topic this service consumes |
| `UNIT_STATUS_TOPIC_NAME` | `unit.status.events` | Unit lifecycle updates (response-time tracking) |
| `RESPONSE_SLA_SECONDS` | `900` | Response-time SLA (dispatched → on scene) |
| `DLT_TOPIC_NAME` | `emergency.events.dlt` | Dead-letter topic |
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
