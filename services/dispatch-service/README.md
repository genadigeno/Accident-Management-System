# Dispatch Service

Part of the [Accident Management System (AMS)](../../README.md). The CAD-style dispatch core:
assigns the **nearest available response unit** to every incident and tracks the unit through
its lifecycle, emitting a status event on each transition.

## What this service does

Consumes all three responder topics and requests one unit per responder type:

| Topic consumed | Unit type dispatched |
|----------------|----------------------|
| `emergency.events` | `AMBULANCE` |
| `law-enforcement.events` | `POLICE_CAR` |
| `fire-rescue.events` | `FIRE_ENGINE` |

For each incident it picks the nearest `AVAILABLE` unit of the required type (Haversine
distance, pessimistic-locked so no unit is ever double-assigned) and records a **dispatch**.
Every lifecycle transition is published to **`unit.status.events`** (Avro `UnitStatusEvent`,
keyed by incident id):

```
DISPATCHED ──► EN_ROUTE ──► ON_SCENE ──► CLEARED
```

- **Call stacking:** when no unit of a type is free the dispatch queues as `WAITING` and is
  assigned FIFO as units clear — exactly what real CAD systems do under load.
- **Simulator:** a scheduler advances active dispatches with randomized realistic timings,
  standing in for real units reporting status over radio/MDT. Disable with
  `DISPATCH_SIM_ENABLED=false` when real units feed the topic.
- **Idempotent:** one dispatch per `(incident cacheId, unit type)` — enforced by a unique
  constraint, so Kafka redeliveries and DLT replays never dispatch twice.
- **Movement:** a unit that clears a call becomes available *at the incident location*.
- Status events are published **after the DB transaction commits** — a rollback can never
  leak a phantom status.

Downstream (roadmap phase 2), the emergency service consumes `unit.status.events` to compute
**response times and SLA breaches** (`dispatched_at → on_scene_at`).

## API

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/units` | The fleet: id, type, state, current position |
| `GET /api/v1/dispatches` | 50 most recent dispatches with all lifecycle timestamps |
| `GET /api/v1/dispatches/active` | Everything not yet cleared (incl. `WAITING` stacked calls) |

```bash
curl http://localhost:8087/api/v1/dispatches/active
```

Metrics: `ams.dispatch.assigned`, `ams.dispatch.queued`, `ams.dispatch.completed`
(all tagged `unitType`), at `/actuator/prometheus`.

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster, Schema Registry, and PostgreSQL — see the
[root README → Installation](../../README.md#-installation).

```bash
java -jar target/dispatch-service.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8087` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `EMERGENCY_TOPIC_NAME` / `LAW_ENFORCEMENT_TOPIC_NAME` / `FIRE_RESCUE_TOPIC_NAME` | responder topics | Topics consumed |
| `UNIT_STATUS_TOPIC_NAME` | `unit.status.events` | Lifecycle events produced |
| `DLT_TOPIC_NAME` | `dispatch.events.dlt` | Dead-letter topic |
| `DISPATCH_SIM_ENABLED` | `true` | Simulator on/off |
| `DISPATCH_SIM_TICK_MS` | `2000` | Simulator tick interval |
| `DISPATCH_STACKING_RETRY_MS` | `3000` | Waiting-queue retry interval |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/accident_management_service` | JDBC URL |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `test` / `postgres` | Database credentials |
| `SPRING_PROFILES_ACTIVE` | `dev` | Active profile (`dev` / `prod`) |

> The database schema (fleet + dispatch log) is created and versioned by **Flyway**; a demo
> fleet of 4 units per type is seeded automatically.

## Tech stack

Java 17 · Spring Boot 3.4.x (Data JPA, Kafka) · Apache Avro + Schema Registry · Lombok · PostgreSQL · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
