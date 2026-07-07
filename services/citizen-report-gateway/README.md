# Citizen Report Gateway

Part of the [Accident Management System (AMS)](../../README.md). The **public front door**:
until now only the load generators produced events — this gateway lets citizens (or partner
systems) report real incidents into the pipeline over plain HTTP.

## What this service does

`POST /api/v1/reports` runs the intake pipeline:

1. **Validation** — type ∈ `CRIMINAL | CAR_ACCIDENT | OTHER_ACCIDENT | FIRE_ACCIDENT`,
   coordinate ranges, address/description length caps → `400` on violations.
2. **API keys** (optional) — `X-API-Key` header checked against `GATEWAY_API_KEYS`
   (comma-separated); unset = open (dev). `401` on missing/invalid key.
3. **Rate limiting** — per reporter (API key, else client address),
   `GATEWAY_RATE_LIMIT`/minute → `429` beyond it.
4. **Duplicate hint** — asks the [correlation service](../incident-correlation-service) whether
   an open incident already exists at that location; if so the response says *"already reported
   (N reports) — your information has been added"*. Best-effort: a correlation outage never
   blocks intake. The report is still taken either way — extra callers add information, exactly
   like a real 911 center.
5. **Confirmed publish** — the Avro report goes to `accident.events` (keyed by its new
   `cacheId`); if Kafka does not acknowledge within 5s the caller gets `503` and nothing is lost
   silently.

```bash
curl -X POST http://localhost:8090/api/v1/reports \
  -H "Content-Type: application/json" \
  -d '{"type":"CAR_ACCIDENT","address":"12 Main St","latitude":41.71,"longitude":44.81,"description":"two cars collided"}'
# 202 {"reportId":"…","duplicateOf":null,"message":"Report accepted. …"}
```

From there the report flows through the whole system: routing → responder persistence →
dispatch → correlation → notifications.

Metrics: `ams.gateway.reports_accepted`, `ams.gateway.rate_limited`.

## Build & run

```bash
mvn clean package
java -jar target/citizen-report-gateway.jar
```

Requires Kafka + Schema Registry (no database).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8090` | HTTP port |
| `BOOTSTRAP_SERVERS` / `SCHEMA_REGISTRY_URL` | localhost | Kafka / Schema Registry |
| `SOURCE_TOPIC_NAME` | `accident.events` | Topic reports are published to |
| `GATEWAY_API_KEYS` | *(empty = gate disabled)* | Comma-separated accepted `X-API-Key` values |
| `GATEWAY_RATE_LIMIT` | `10` | Reports per reporter per minute |
| `GATEWAY_CORRELATION_URL` | `http://localhost:8089` | Correlation service (blank disables the hint) |

## Tech stack

Java 17 · Spring Boot 3.4.x (Web, Kafka) · Apache Avro + Schema Registry · Caffeine · Lombok · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
