# AMS Services

The Spring Boot applications that make up the [Accident Management System](../README.md)
pipeline: a router fans incidents out to four responder services, and a second tier
(dispatch, notification, correlation) plus a public intake gateway build the full CAD loop
on top:

```
                          ┌───────────────────────┐
  accident.events ─────▶ │ accident-event-stream │──▶ law-enforcement.events ──▶ law-enforcement-service ──▶ PostgreSQL
 (from the bombarder      │       (router)        │──▶ emergency.events ────────▶ emergency-service ────────▶ PostgreSQL
  or the uiapp)           │     Kafka Streams     │──▶ fire-rescue.events ──────▶ firerescue-service ───────▶ PostgreSQL
                          └───────────────────────┘──▶ statistics.events ───────▶ statistics-service ───────▶ PostgreSQL
                                    │
                                    ├──▶ accident.events.sensitive   (geo-fenced incidents)
                                    ├──▶ accident.events.fraud       (rapid-repeat alerts)
                                    └──▶ accident.events.dlt         (undeserializable records)
```

## At a glance

| Service | Role | Port | Consumes | Key APIs |
|---------|------|:----:|----------|----------|
| [accident-event-stream](accident-event-stream) | The brain — routes each incident by `ACCIDENT_TYPE` to the responder topics; geo-fencing, fraud detection, windowed statistics | `8080` | `accident.events` | actuator only |
| [law-enforcement-service](law-enforcement-service) | Persists police incidents; automatic **BOLO** threat classification | `28089` | `law-enforcement.events` | `GET /api/v1/bolo` |
| [emergency-service](emergency-service) | Persists ambulance incidents; **nearest-hospital** lookup; **response-time/SLA** tracking from `unit.status.events` | `18089` | `emergency.events`, `unit.status.events` | `GET /api/v1/hospitals/nearby`, `GET /api/v1/response-times[/summary]` |
| [firerescue-service](firerescue-service) | Persists fire incidents; **building-plan** lookup (DB-backed + stub fallback, importable) and **nearby-hydrant** lookup | `38089` | `fire-rescue.events` | `GET/POST /api/v1/buildings/plan`, `GET /api/v1/hydrants/nearby` |
| [statistics-service](statistics-service) | Persists windowed per-type counts; metrics, rollups, trends, CSV reports | `48089` | `statistics.events` | `GET /api/v1/stats/{by-type,summary,recent,hourly,daily,weekly,trend,peak-hours}`, `GET /api/v1/reports/daily.csv` |
| [dispatch-service](dispatch-service) | CAD core — assigns the nearest available unit per incident, tracks `DISPATCHED → EN_ROUTE → ON_SCENE → CLEARED`, emits `unit.status.events` | `8087` | all three responder topics | `GET /api/v1/units`, `GET /api/v1/dispatches[/active]` |
| [notification-service](notification-service) | Fans in every alert stream (BOLO / SLA / fraud / geo-fence) → dedup, rate-limit, deliver to channels (log, webhook, …) | `8088` | `bolo.alerts`, `sla.alerts`, `accident.events.fraud`, `accident.events.sensitive` | `GET /api/v1/notifications[/summary]` |
| [incident-correlation-service](incident-correlation-service) | Merges duplicate reports of the same real-world incident (type + ~150 m grid cell + time window); emits `incident.events` (`OPENED/UPDATED/CLOSED`) | `8089` | `accident.events` | `GET /api/v1/incidents[/{id}\|/nearby]` |
| [citizen-report-gateway](citizen-report-gateway) | Public intake: validates, API-keys and rate-limits citizen reports, adds a duplicate hint, and publishes into the pipeline | `8090` | — (produces `accident.events`) | `POST /api/v1/reports` |
| [search-service](search-service) | Indexes every incident into Elasticsearch for full-text, geo and time-range search — including past-incident lookup near a location | `8091` | `accident.events` | `GET /api/v1/search`, `GET /api/v1/history/nearby` |
| [enrichment-service](enrichment-service) | Enriches each incident with weather (Open-Meteo) and district, republishing `accident.events.enriched` | `8092` | `accident.events` | `GET /api/v1/enriched/recent` |

## The services

### accident-event-stream — the router

A Kafka Streams application (exactly-once) that reads every reported incident and fans it out:
`CAR_ACCIDENT` and `FIRE_ACCIDENT` go to all three responders, `CRIMINAL` to police + emergency,
`OTHER_ACCIDENT` to police only — and everything to statistics. On top of routing it runs three
stream features: **geo-fencing** (incidents at sensitive addresses are additionally published to
`accident.events.sensitive`), **fraud detection** (the same location reported more than a
threshold within a time window raises an alert on `accident.events.fraud`), and **windowed
statistics** feeding the statistics service. Records that cannot be deserialized go to the
dead-letter topic instead of crashing the stream, and a `kafkaStreams` health indicator ties the
topology state into the liveness probe.

### law-enforcement-service

Batch-consumes police events and persists them idempotently to its own PostgreSQL database.
Every incident description is scanned by the **BOLO detector** ("Be On the Lookout"): weapon /
hostage keywords classify it `CRITICAL`, robbery / stolen-vehicle keywords `HIGH` — raising a
metric and a log alert, with the 50 most recent active BOLOs served at `GET /api/v1/bolo`.

### emergency-service

Batch-consumes ambulance/emergency events and persists them idempotently. Also exposes a
**nearest-hospital** lookup — `GET /api/v1/hospitals/nearby?lat=..&lng=..&radius=..` — backed by
the free OpenStreetMap Overpass API and sorted by distance, so responders can be directed to the
closest hospital.

### firerescue-service

Batch-consumes fire-rescue events and persists them idempotently. Also exposes a **building-plan**
lookup — `GET /api/v1/buildings/plan?address=..` — via a pluggable provider (stub included), so
crews can check floor plans / hazards for the address they are responding to.

### statistics-service

Consumes the router's windowed aggregates and upserts them by `(type, window start, window end)`.
Serves real-time metrics: totals per type (`/api/v1/stats/by-type`), a headline summary
(`/api/v1/stats/summary`), and the most recent windows (`/api/v1/stats/recent`).

## Shared conventions

All services follow the same production patterns:

- **Configuration via environment variables** with sensible localhost defaults —
  `BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `SERVER_PORT`, `POSTGRES_URL` /
  `POSTGRES_USER` / `POSTGRES_PASSWORD` (see each service's README for its full table).
- **Avro data contract** from [`libs/ams-schemas`](../libs/ams-schemas) via Confluent Schema
  Registry; consumers read `read_committed` with manual acknowledgement.
- **Reliability:** exponential-backoff retries, dead-letter topics with the original payload and
  `DLT_*` headers preserved (replayable with [dlt-replay](../tools/dlt-replay-app)), and
  idempotent persistence (Kafka coordinates + unique index) so redeliveries never duplicate rows.
- **One database per service**, schema owned by Flyway migrations.
- **Observability:** Spring Boot Actuator with liveness/readiness probes and a Prometheus scrape
  endpoint at `/actuator/prometheus`, tagged `application=<service-name>`.

## Build & run

From the repository root ([full instructions](../README.md#-installation)):

```bash
mvn clean package                 # builds every service (root aggregator pom)

# start the router first, then the responders — one terminal each
java -jar services/accident-event-stream/target/accident-event-stream.jar
java -jar services/emergency-service/target/emergency-service.jar
java -jar services/law-enforcement-service/target/law-enforcement-service.jar
java -jar services/firerescue-service/target/fire-rescue-service.jar
java -jar services/statistics-service/target/statistics-service.jar
```

Or containerized — see the root [`Dockerfile`](../Dockerfile) and the
[Kubernetes guide](../k8s/README.md).
