# Search Service

Part of the [Accident Management System (AMS)](../../README.md). Indexes every reported incident
into **Elasticsearch** so the history can be searched by **full text**, **geography**, and **time** —
including the original spec's "past incident correlation" (*"this is the 3rd robbery at this bank
in 2 months → increase patrols"*).

## What this service does

Consumes every report (`accident.events`) and upserts a document into the `ams-incidents` index
(document id = the report's `cacheId`, so re-indexing is idempotent). Each document has the
accident type, description (full-text), address, a `geo_point` location, and the indexing
timestamp.

It talks to Elasticsearch over its **plain HTTP API** (Spring `RestClient` + JSON query DSL)
rather than the typed Java client, so it is decoupled from the ES client/server version pair
(the project's ES server is 9.x; the index / search / `geo_distance` endpoints are stable across
8.x–9.x). The index and its mapping are created on startup if missing.

## API

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/search?q=&type=&days=&size=` | Full-text search over descriptions, filtered by type / recency |
| `GET /api/v1/history/nearby?lat=&lng=&radiusMeters=&type=&days=&size=` | Past incidents within a radius — `total` answers "how many times here", `hits` lists them |

```bash
curl "http://localhost:8091/api/v1/search?q=robbery&type=CRIMINAL&days=60"
curl "http://localhost:8091/api/v1/history/nearby?lat=41.7&lng=44.8&radiusMeters=200&type=CRIMINAL&days=60"
```

Elasticsearch reachability is surfaced under `/actuator/health` (`elasticsearch` component);
`ams.search.indexed` counts indexed documents. Because indexing is idempotent, an ES outage just
retries — nothing is lost — and a Kibana dashboard over `ams-incidents` comes for free.

## Build & run

```bash
mvn clean package
java -jar target/search-service.jar
```

Requires Kafka + Schema Registry + a reachable Elasticsearch (no database of its own).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8091` | HTTP port |
| `BOOTSTRAP_SERVERS` / `SCHEMA_REGISTRY_URL` | localhost | Kafka / Schema Registry |
| `SOURCE_TOPIC_NAME` | `accident.events` | Reports consumed and indexed |
| `DLT_TOPIC_NAME` | `search.events.dlt` | Dead-letter topic |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | ES endpoint (any reachable node) |
| `ELASTICSEARCH_INDEX` | `ams-incidents` | Index name |
| `SPRING_PROFILES_ACTIVE` | `dev` | `dev` / `prod` |

## Tech stack

Java 17 · Spring Boot 3.4.x (Web, Kafka) · Elasticsearch (HTTP API) · Apache Avro + Schema Registry · Lombok · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
