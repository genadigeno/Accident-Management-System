# Accident Event Stream (Router)

The routing core of the [Accident Management System (AMS)](../../README.md). See the root README
for the full architecture, the plain-language overview, and end-to-end setup.

## What this service does

A **Kafka Streams** application that reads incidents from the source topic (`accident.events`)
and routes each one to the correct responder topics based on its `ACCIDENT_TYPE`. Every event
is also forwarded to the statistics pipeline, where it is aggregated into time windows. Records
that cannot be deserialized are sent to a dead-letter topic (`accident.events.dlt`) instead of
crashing the stream.

### Routing rules

| Incident type    | `law-enforcement.events` | `emergency.events` | `fire-rescue.events` |
|------------------|:---:|:---:|:---:|
| `CAR_ACCIDENT`   | ✅ | ✅ | ✅ |
| `FIRE_ACCIDENT`  | ✅ | ✅ | ✅ |
| `CRIMINAL`       | ✅ | ✅ | — |
| `OTHER_ACCIDENT` | ✅ | — | — |

All event types are additionally streamed to `statistics.events`.

## Features

### Geo-fencing (sensitive zones)

Incidents whose address falls inside a configured **sensitive zone** (government buildings,
embassies, airports, ...) are routed to an **additional** topic, `accident.events.sensitive`,
on top of their normal responder routing — so a dedicated handler (e.g. counter-terrorism) can
subscribe to just those. Each match also raises a `ams.geofence.sensitive` metric and a WARN alert.

Matching is a case-insensitive keyword check against the address; the keyword list is configurable:

| Variable | Default |
|----------|---------|
| `SENSITIVE_TOPIC_NAME` | `accident.events.sensitive` |
| `GEOFENCE_KEYWORDS` | `government,embassy,parliament,airport,military,nuclear,courthouse` |

### Fraud detection (rapid repeats)

The same location reported too many times in a short window is likely fake. Done **in-stream**
with a Kafka Streams windowed count by location (no external rate-limiter): when a location
exceeds the threshold within the window, an alert is published to a fraud topic and a
`ams.fraud.flagged` metric + WARN alert are raised.

| Variable | Default | Description |
|----------|---------|-------------|
| `FRAUD_TOPIC_NAME` | `accident.events.fraud` | topic the alerts are published to |
| `FRAUD_WINDOW_MINUTES` | `5` | tumbling window size |
| `FRAUD_THRESHOLD` | `5` | a location reported more than this many times in the window is flagged |

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster and Schema Registry — see the
[root README → Installation](../../README.md#-installation). Start this service **before** the
downstream responder services.

```bash
java -jar target/accident-event-stream.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8080` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `SOURCE_TOPIC_NAME` | `accident.events` | Incoming incident topic |
| `DLT_SOURCE_TOPIC_NAME` | `accident.events.dlt` | Dead-letter topic for unparseable records |
| `EMERGENCY_SERVICE_TOPIC_NAME` | `emergency.events` | Emergency sink topic |
| `POLICE_SERVICE_TOPIC_NAME` | `law-enforcement.events` | Police sink topic |
| `FIRE_RESCUE_SERVICE_TOPIC_NAME` | `fire-rescue.events` | Fire-rescue sink topic |
| `STATISTICS_SERVICE_TOPIC_NAME` | `statistics.events` | Statistics sink topic |
| `STATE_STORE_CACHE_MAX_SIZE` | `0` | Kafka Streams state-store cache size (bytes) |
| `STATE_DIRECTORY` | `./state-dir` | Kafka Streams local state directory |
| `AVRO_SUBJECT_VERSION` | `latest` | Avro subject version to read |

## Tech stack

Java 17 · Spring Boot 3.4.x · Apache Kafka Streams · Apache Avro + Schema Registry · Lombok · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
