# Notification Service

Part of the [Accident Management System (AMS)](../../README.md). The single place where
operational alerts become **pushes**: it fans in every alert-class topic and delivers each alert
to the enabled channels, exactly once.

## What this service does

| Topic consumed | Alert source | Produced by |
|----------------|--------------|-------------|
| `bolo.alerts` | `BOLO` (HIGH/CRITICAL threat keywords) | law-enforcement-service |
| `sla.alerts` | `SLA` (response-time breaches) | emergency-service |
| `accident.events.fraud` | `FRAUD` (rapid-repeat locations) | accident-event-stream |
| `accident.events.sensitive` | `GEOFENCE` (incidents at sensitive addresses) | accident-event-stream |

- **Exactly-once notifications:** every alert carries (or derives) a stable `dedupKey`; a unique
  database constraint makes redeliveries, DLT replays, and full-topic replays idempotent forever.
- **Rate limiting:** at most `NOTIFICATION_RATE_LIMIT` notifications per **source** per minute
  reach the channels; the rest are still recorded but marked `rateLimited` — an alert storm
  can't melt the channels.
- **Channels** are pluggable ([`NotificationChannel`](src/main/java/ams/notification/channel/NotificationChannel.java)):
  - `log` — always on (WARN entries).
  - `webhook` — POSTs each notification as JSON; enabled when `NOTIFICATION_WEBHOOK_URL` is set
    (Slack/Teams/incident tooling).
  - SMTP / Telegram / SMS: implement the interface as a `@Component` gated by
    `@ConditionalOnProperty` — the service picks up every enabled channel automatically.
- Per-notification delivery results are recorded (e.g. `log:sent,webhook:failed`).

## API

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/notifications` | 50 most recent notifications (source, severity, channels, …) |
| `GET /api/v1/notifications/summary` | Counts per source and severity |

Metrics: `ams.notifications.sent`, `ams.notifications.deduplicated`,
`ams.notifications.rate_limited` (all tagged `source`), at `/actuator/prometheus`.

## Build

```bash
mvn clean package
```

## Run

Requires a running Kafka cluster, Schema Registry, and PostgreSQL — see the
[root README → Installation](../../README.md#-installation).

```bash
java -jar target/notification-service.jar
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `60089` | HTTP port |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `BOLO_ALERTS_TOPIC_NAME` / `SLA_ALERTS_TOPIC_NAME` | `bolo.alerts` / `sla.alerts` | Structured alert topics |
| `FRAUD_TOPIC_NAME` / `SENSITIVE_TOPIC_NAME` | router alert topics | Fraud / geo-fence fan-in |
| `DLT_TOPIC_NAME` | `notification.events.dlt` | Dead-letter topic |
| `NOTIFICATION_RATE_LIMIT` | `30` | Channel deliveries per source per minute |
| `NOTIFICATION_WEBHOOK_URL` | *(unset)* | Enables the webhook channel |
| `NOTIFICATION_WEBHOOK_TIMEOUT_MS` | `3000` | Webhook connect/read timeout |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/accident_management_service` | JDBC URL |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `test` / `postgres` | Database credentials |
| `SPRING_PROFILES_ACTIVE` | `dev` | Active profile (`dev` / `prod`) |

## Tech stack

Java 17 · Spring Boot 3.4.x (Data JPA, Kafka) · Apache Avro + Schema Registry · Caffeine · Lombok · PostgreSQL · Docker

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
