# UI App (AMS Dashboard)

A real-time operations dashboard for the [Accident Management System](../../README.md). It
consumes the Kafka event stream, generates test load, monitors the backend services, and shows
live analytics — all pushed to the browser over WebSocket (STOMP).

The backend is **Spring Boot** and is **API-first**: the bundled page is only a reference
client, so the front-end can be rewritten/hosted separately (see [Front-end / back-end split](#front-end--back-end-split)).

## Features

- **Live event feed** — every accident on `accident.events` is streamed to the browser
  (newest 50) with running per-type counters.
- **Event generator** — generate up to 10,000 random events; each batch is dispatched
  asynchronously and its **send status** (produced / failed / progress) is tracked live.
- **Service discovery** — actively health-polls each backend service and shows it
  **UP / DEGRADED / DOWN** with measured latency.
- **Real-time analytics** — events/sec, per-type distribution (chart), top locations, and
  downstream **sensitive-zone** and **fraud** alert counts, updated every second.

## API contract

REST (`/api/v1`):

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/messages` (`total=N`) | Generate N random events; returns `202` + the batch. |
| `GET`  | `/messages/batches` | Recent send batches. |
| `GET`  | `/messages/batches/{id}` | One send batch. |
| `GET`  | `/register` | Current health of all monitored services. |
| `POST` | `/register` | Register a service to monitor (`ServiceDto`). |
| `GET`  | `/register/{name}` | Heartbeat for a service. |

WebSocket — STOMP endpoint `/discover`, subscribe to:

| Topic | Payload |
|-------|---------|
| `/topic/events` | each accident event |
| `/topic/send-status` | send-batch status updates |
| `/topic/service-discovery` | service health snapshot |
| `/topic/analytics` | aggregated analytics snapshot (every 1s) |

## How it works

```
accident.events ─▶ EventStreamConsumer ─▶ /topic/events     (live feed)
                                        └▶ AnalyticsService ─▶ /topic/analytics
accident.events.sensitive / .fraud ─▶ EventStreamConsumer ─▶ analytics counts
browser "Send" ─▶ POST /messages ─▶ MessageService ─▶ accident.events
                                  └▶ SendBatchRegistry ─▶ /topic/send-status
ServiceCheck (scheduled) ─▶ GET each service /actuator/health ─▶ /topic/service-discovery
```

## Run locally

Start the [infrastructure](../../README.md#-installation) (Kafka + Schema Registry), build, then run:

```bash
mvn -f tools/uiapp/pom.xml clean package
java -jar tools/uiapp/target/uiapp-0.0.1-SNAPSHOT.jar
```

Open **http://localhost:9000**. Click **Send** to generate events and watch the feed, batch
status, services panel, and analytics update live.

## Front-end / back-end split

The page (`templates/index.html` + `static/app.js`) is plain static HTML/JS that talks to the
backend **only** through the REST + WebSocket contract above — so it can run as a separate app:

1. Run this module as the **back-end only** (it still serves the reference page, but you can
   ignore it). CORS for `/api/**` is enabled via `frontend.cors.allowed-origins`.
2. Host the front-end separately (any static host, or a React/Vue/Svelte SPA). Point it at the
   backend by setting `window.AMS_BACKEND` before `app.js` loads:
   ```html
   <script>window.AMS_BACKEND = 'http://localhost:9000';</script>
   <script src="app.js"></script>
   ```
   `app.js` derives both the REST base URL and the `ws(s)://…/discover` URL from it (defaults to
   same-origin when unset).
3. In production, pin `FRONTEND_ORIGINS` to the front-end's origin instead of `*`.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `9000` | HTTP port for the dashboard |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka broker list |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `SOURCE_TOPIC_NAME` | `accident.events` | Source topic (consumed + produced) |
| `SENSITIVE_TOPIC_NAME` | `accident.events.sensitive` | Geo-fence alerts (analytics) |
| `FRAUD_TOPIC_NAME` | `accident.events.fraud` | Fraud alerts (analytics) |
| `KAFKA_GROUP_ID` | `ui-app-dashboard` | Consumer group id |
| `FRONTEND_ORIGINS` | `*` | Allowed CORS origins for `/api/**` |
| `DISCOVERY_POLL_MS` | `5000` | Service health-poll interval (ms) |
| `DISCOVERY_TIMEOUT_MS` | `2000` | Per-probe timeout (ms) |
| `DISCOVERY_DEGRADED_MS` | `500` | Latency above which a service is DEGRADED (ms) |
| `ROUTER_URL` / `EMERGENCY_URL` / `LAW_ENFORCEMENT_URL` / `FIRERESCUE_URL` / `STATISTICS_URL` | localhost ports | Monitored service base URLs |

## Tech stack

Spring Boot 3.4.x (Web, WebSocket, Thymeleaf) · Spring for Apache Kafka · Apache Avro +
Schema Registry (via [`ams-schemas`](../../libs/ams-schemas)) · Caffeine · Bootstrap 5 ·
Chart.js · Lombok
