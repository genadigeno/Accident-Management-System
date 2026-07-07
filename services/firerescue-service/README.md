# Fire Rescue Service

Part of the [Accident Management System (AMS)](../../README.md). See the root README for the
full architecture, the plain-language overview, and end-to-end setup.

## What this service does

Consumes **fire-rescue events** from Kafka (default topic `fire-rescue.events`), maps them, and
persists them to PostgreSQL. Records that cannot be processed are routed to a dead-letter
topic (`fire-rescue.events.dlt`) so nothing is silently lost.

## Features

### Building-plan lookup

Gives crews the fire-relevant building information for an address — **escape routes** and
**gas-line locations** — so they arrive prepared.

```bash
curl "http://localhost:38089/api/v1/buildings/plan?address=12%20Oak%20Street"
# { "address":"12 Oak Street", "floors":9,
#   "fireEscapeRoutes":["Stairwell ...","External fire escape ..."],
#   "gasLineLocations":["Main gas shutoff ...","Gas riser ..."], "source":"stub" }
```

The lookup is behind a [`BuildingPlanProvider`](src/main/java/ams/firerescue/building/BuildingPlanProvider.java)
adapter. The **primary** provider is now **database-backed**: a plan imported for an address is
served from PostgreSQL (`source: "db"`); addresses that aren't stored fall back to the deterministic
in-memory **stub** (`source: "stub"`), so the endpoint always has an answer. A blank address returns `400`.

Import a real plan (thereafter served from the DB):

```bash
curl -X POST http://localhost:38089/api/v1/buildings/plan -H "Content-Type: application/json" \
  -d '{"address":"12 Oak Street","floors":9,
       "fireEscapeRoutes":["Stairwell A — floors 1-9","External fire escape — floor 3"],
       "gasLineLocations":["Main shutoff — basement NW"]}'
```

### Nearby fire hydrants

Finds fire hydrants near an incident so crews know their water supply. Backed by the free
OpenStreetMap Overpass API (`emergency=fire_hydrant`), sorted by distance, cached per location.

```bash
curl "http://localhost:38089/api/v1/hydrants/nearby?lat=41.7151&lng=44.8271&radius=500"
# [ { "ref":"hydrant", "latitude":41.71, "longitude":44.83, "distanceMeters":120 }, ... ]
```

`radius` is metres (50–10000, default 500). Invalid coordinates return `400`; an unreachable
provider degrades to `502`.

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster, Schema Registry, and PostgreSQL — see the
[root README → Installation](../../README.md#-installation).

```bash
java -jar target/fire-rescue-service.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `38089` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `MAIN_TOPIC_NAME` | `fire-rescue.events` | Topic this service consumes |
| `DLT_TOPIC_NAME` | `fire-rescue.events.dlt` | Dead-letter topic |
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
