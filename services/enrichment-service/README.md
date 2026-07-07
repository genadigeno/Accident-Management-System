# Enrichment Service

Part of the [Accident Management System (AMS)](../../README.md). Adds **context** to incidents:
each reported event is augmented with the **weather at its location** and a **district** label,
and republished to `accident.events.enriched` for downstream use (analytics, the router's
enriched path, dashboards).

## What this service does

Consumes the live incident stream (`accident.events`, from the tip — enrichment is
forward-looking) and for each event publishes an `EnrichedAccidentEvent`:

- **Weather** from the free [Open-Meteo](https://open-meteo.com) API (no API key) — condition,
  temperature, precipitation. Cached by ~1 km coordinate for a short TTL so a burst of nearby
  incidents makes at most one call; a slow/unreachable API degrades to `weatherCondition =
  "unknown"` and **never stalls the stream**.
- **District** from a pluggable [`DistrictResolver`](src/main/java/ams/enrichment/district/DistrictResolver.java).
  The default is offline and deterministic (a coordinate grid); a reverse-geocoding
  implementation (e.g. Nominatim) can be swapped in behind the interface.

Enrichment never fails an event — a missing weather reading just yields `"unknown"`.

## API

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/enriched/recent` | The 50 most recently enriched events (in-memory; the service has no DB) |

```bash
curl http://localhost:8092/api/v1/enriched/recent
```

Metrics: `ams.enrichment.enriched`, `ams.enrichment.weather_unavailable`, at `/actuator/prometheus`.

## Build & run

```bash
mvn clean package
java -jar target/enrichment-service.jar
```

Requires Kafka + Schema Registry (no database). Weather needs outbound HTTPS to Open-Meteo; it
degrades gracefully if that is blocked.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8092` | HTTP port |
| `BOOTSTRAP_SERVERS` / `SCHEMA_REGISTRY_URL` | localhost | Kafka / Schema Registry |
| `SOURCE_TOPIC_NAME` | `accident.events` | Incidents consumed |
| `ENRICHED_TOPIC_NAME` | `accident.events.enriched` | Enriched events produced |
| `DLT_TOPIC_NAME` | `enrichment.events.dlt` | Dead-letter topic |
| `OPEN_METEO_URL` | `https://api.open-meteo.com/v1/forecast` | Weather API endpoint |
| `SPRING_PROFILES_ACTIVE` | `dev` | `dev` / `prod` |

## Tech stack

Java 17 · Spring Boot 3.4.x (Web, Kafka) · Open-Meteo · Caffeine · Apache Avro + Schema Registry · Lombok · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
