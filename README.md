<div align="center">

<img src="logo.png" alt="AMS logo" width="130"/>

# Accident Management System (AMS)

**An event-driven system that routes emergency incidents to the right responders — automatically.**

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/projects/jdk/17/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.x-6DB33F.svg?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streams-231F20.svg?logo=apachekafka)](https://kafka.apache.org/documentation/streams/)
[![Avro](https://img.shields.io/badge/Apache%20Avro-Schema%20Registry-1A73E8.svg)](https://avro.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

</div>

---

## 📖 What is it?

Imagine a city's **emergency dispatch center**. When something happens — a car crash, a fire, a break-in — a single report comes in. A dispatcher has to instantly decide *who* needs to know: the police, the ambulance, the fire brigade, or several of them at once. They also keep a tally of how many incidents of each kind happened, so the city can plan ahead.

**The Accident Management System does exactly this job, automatically and at scale.**

- An incident is reported (in this project, a generator app simulates thousands of them).
- The system reads the **type** of each incident and instantly forwards it to the right teams.
- Each team's service records what it received so nothing is lost.
- A statistics service keeps a live count of incidents, grouped by type and time.

It's built so that if one team's service is busy or briefly fails, the others keep working — and no incident is silently dropped. This is the core idea behind **event-driven architecture**: services react to a stream of events independently, instead of all depending on one central program.

> 💡 **Who is this for?** It's a portfolio / learning project demonstrating a realistic, production-style streaming system. Nothing here connects to real emergency services.

---

## 🧭 How it works (the 30-second version)

```
                      ┌──────────────────────┐
   simulated          │  accident-event-     │   routes by ACCIDENT_TYPE
   incidents ───────▶ │  stream              │ ───────────────┬───────────────┐
 (stream-bombarder)   │  (Kafka Streams)     │                │               │
                      └──────────┬───────────┘                │               │
                                 │                            ▼               ▼
                  ┌──────────────┼───────────────┐     ┌────────────┐  ┌──────────────┐
                  ▼              ▼               ▼     │ statistics │  │     other    │
          ┌──────────────┐ ┌──────────┐ ┌────────────┐ │  service   │  │  responders  │
          │ emergency-   │ │ law-     │ │ firerescue-│ │ (windowed  │  └──────────────┘
          │ service      │ │ enforce. │ │ service    │ │  counts)   │
          └──────┬───────┘ └────┬─────┘ └─────┬──────┘ └──────┬─────┘
                 ▼              ▼             ▼              ▼
              PostgreSQL    PostgreSQL    PostgreSQL     PostgreSQL
```

**Routing rules** (decided by `accident-event-stream` based on the incident's `ACCIDENT_TYPE`):

| Incident type    | Police (law-enforcement) | Emergency (ambulance) | Fire-rescue |
|------------------|:---:|:---:|:---:|
| `CAR_ACCIDENT`   | ✅ | ✅ | ✅ |
| `FIRE_ACCIDENT`  | ✅ | ✅ | ✅ |
| `CRIMINAL`       | ✅ | ✅ | — |
| `OTHER_ACCIDENT` | ✅ | — | — |

Every incident — regardless of type — is also fed to the **statistics-service**, which aggregates counts per type over time windows using Kafka Streams. If any message can't be processed, it is sent to a **dead-letter topic (DLT)** instead of being lost, so it can be inspected and replayed later.

---

## 📦 Modules

| Module | What it does |
|--------|--------------|
| [`services/accident-event-stream`](services/accident-event-stream) | The brain. A Kafka Streams app that reads the source topic and routes each event to the correct responder topics. |
| [`services/emergency-service`](services/emergency-service) | Consumes ambulance/emergency events and persists them to PostgreSQL. |
| [`services/law-enforcement-service`](services/law-enforcement-service) | Consumes police events and persists them to PostgreSQL. |
| [`services/firerescue-service`](services/firerescue-service) | Consumes fire-rescue events and persists them to PostgreSQL. |
| [`services/statistics-service`](services/statistics-service) | Consumes aggregated statistics events and persists windowed counts to PostgreSQL. |
| [`services/dispatch-service`](services/dispatch-service) | The dispatcher. Assigns the nearest available response unit (police car / ambulance / fire engine) to each incident and tracks it to completion. |
| [`services/notification-service`](services/notification-service) | The alerter. Collects every operational alert (BOLO, SLA breaches, fraud, sensitive zones) and delivers each one exactly once to the configured channels. |
| [`libs/ams-schemas`](libs/ams-schemas) | The shared data contract — Apache Avro schemas published to the Schema Registry and used by every service. |
| [`tools/stream-bombarder-app`](tools/stream-bombarder-app) | A load generator that floods the source topic with realistic random incidents for testing. |
| [`tools/dlt-replay-app`](tools/dlt-replay-app) | Replays dead-letter records back to their source topic once the underlying issue is fixed. |
| [`tools/uiapp`](tools/uiapp) | A live Thymeleaf + WebSocket dashboard: streams incidents to the browser and generates test events. |

---

> 📜 **The message format is shared, not duplicated.** Every message that crosses Kafka is
> defined once as an Avro schema in [`libs/ams-schemas`](libs/ams-schemas) and published as a
> library, so producers and consumers compile against the same classes. See its
> [README](libs/ams-schemas/README.md) for how the data contract and Schema Registry work.

## 🛠️ Technology stack

- **Language:** Java 17
- **Framework:** Spring Boot 3.4.x (Web, Data JPA, Kafka)
- **Streaming:** Apache Kafka + Kafka Streams
- **Data contracts:** Apache Avro + Confluent Schema Registry
- **Mapping:** MapStruct · **Boilerplate:** Lombok
- **Database:** PostgreSQL
- **Packaging & infra:** Docker, Docker Compose, Kubernetes (Kustomize)
- **Testing:** JUnit 5, Spring Kafka Test, Testcontainers
- **Observability:** Spring Boot Actuator, Micrometer, Prometheus, Grafana
- **CI/CD:** GitHub Actions

---

## 🚀 Installation

### Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| JDK | 17+ | `JAVA_HOME` must be set |
| Maven | 3.8+ | or use the bundled `mvnw` wrapper if present |
| Docker + Docker Compose | latest | runs Kafka, Schema Registry, PostgreSQL |
| Git | latest | |

### 1. Clone

```bash
git clone https://github.com/<your-username>/accident-management-system.git
cd accident-management-system
```

### 2. Start the tools (infrastructure)

The supporting **tools** run in Docker — a 3-broker Kafka cluster (KRaft), Schema Registry,
PostgreSQL, Prometheus and Grafana. The application services do **not** run in Docker.

```bash
docker compose up -d
docker compose ps          # wait until healthy
```

| Tool | URL / endpoint |
|------|----------------|
| Kafka (host listeners) | `localhost:9091,localhost:9092,localhost:9093` |
| Schema Registry | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 — login `admin` / `admin` |
| PostgreSQL | `localhost:5432` — db `accident_management_service`, user `test`, pass `postgres` |

Stop them with `docker compose down` (add `-v` to also wipe data).

### 3. Build the services

The repo has a root aggregator `pom.xml`, so one command builds every module in order:

```bash
mvn clean package
```

> ℹ️ The shared `ams-schemas` Avro contract lives in [`libs/ams-schemas`](libs/ams-schemas) and is
> built first. To build one module: `mvn -f services/emergency-service/pom.xml clean package`.

### 4. Start the services

Each service is a plain **`java -jar` on your host** (not containerised). Start the **router
first**, then the responders — one terminal each:

```bash
# 1) The router (Kafka Streams)
java -jar services/accident-event-stream/target/accident-event-stream.jar

# 2) The responder services
java -jar services/emergency-service/target/emergency-service.jar
java -jar services/law-enforcement-service/target/law-enforcement-service.jar
java -jar services/firerescue-service/target/fire-rescue-service.jar
java -jar services/statistics-service/target/statistics-service.jar
```

They default to the tools above (localhost Kafka/Schema Registry, and `test`/`postgres` on the
Docker PostgreSQL). Override any setting via env vars or `-D` args — see [Configuration](#-configuration).
On Windows, the [`infra/run-*.bat`](infra) helpers wrap these commands with local defaults.

---

## ▶️ Usage

Once the router and at least one responder service are running, generate traffic with the **stream-bombarder**:

```bash
java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --scale=5
```

- `--scale` multiplies how many events are sent per burst. Start small.
- ⚠️ **Caution:** high `--scale` values (or many instances) can generate **millions** of messages and saturate your CPU/disk. Recommended range: `1`–`10`.

The other tools — the **uiapp** live dashboard and the **dlt-replay** recovery tool — are covered
in [🧰 Tools](#-tools) below.

Now watch the routing happen:
- The **router** logs each incoming event and which topics it forwards to.
- Each **responder service** logs the batches it consumes and persists.
- Query PostgreSQL to see stored incidents:

```bash
docker exec -it ams-db psql -U test -d accident_management_service -c \
  "SELECT count(*) FROM emergency_accidents;"
```

---

## 🧰 Tools

Three tools live under [`tools/`](tools). All are built by the root `mvn clean package`; each has
its own README with the full reference.

### Stream bombarder — load generator

Floods the source topic with random incidents to load-test the pipeline. Run several instances in
parallel for more load.

```bash
# continuous load
java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --scale=5

# a bounded run: exactly 10,000 events, then exit
java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --count=10000
```

| Option | Default | Description |
|--------|---------|-------------|
| `--scale=N` | `1` | Burst-size multiplier |
| `--max-burst=N` | `100` | Max events per burst (before scale) |
| `--interval-ms=N` | `1000` | Max delay between bursts (ms) |
| `--count=N` | `0` | Stop after N events (`0` = unlimited) |
| `--duration-sec=N` | `0` | Stop after N seconds (`0` = unlimited) |
| `--help`, `-h` | | Full usage |

Settings also resolve from environment variables or a `.env` file (precedence: CLI > env > `.env` >
default). Full details: [`tools/stream-bombarder-app`](tools/stream-bombarder-app).
⚠️ High `--scale` or many instances can generate **millions** of messages — start with `1`–`10`.

### UI dashboard (uiapp) — live monitoring

A Spring Boot + WebSocket dashboard: live event feed, send-batch status, service health
(UP / DEGRADED / DOWN with latency), and real-time analytics (events/sec, per-type, top locations,
sensitive-zone and fraud counts).

```bash
java -jar tools/uiapp/target/uiapp-0.0.1-SNAPSHOT.jar
```

Open **http://localhost:9000**, set an event count, and click **Send**. The backend is API-first
(REST + STOMP/WebSocket) and can be hosted as a **separate front-end app** — set
`window.AMS_BACKEND` and `FRONTEND_ORIGINS` for CORS. Full details: [`tools/uiapp`](tools/uiapp).

### DLT replay — recover failed messages

Replays records from a dead-letter topic back to their source topic (byte-for-byte, with Spring's
internal `DLT_*` headers stripped) once the underlying issue is fixed.

```bash
# inspect first — count only, publish nothing
java -jar tools/dlt-replay-app/target/dlt-replay.jar \
    --source.topic=emergency.events.dlt --target.topic=emergency.events --dry-run=true

# then replay for real
java -jar tools/dlt-replay-app/target/dlt-replay.jar \
    --source.topic=emergency.events.dlt --target.topic=emergency.events
```

| Option | Default | Description |
|--------|---------|-------------|
| `--source.topic` | _(required)_ | Dead-letter topic to read from |
| `--target.topic` | _(required)_ | Topic to republish to |
| `--bootstrap.servers` | `localhost:9092,localhost:9093` | Kafka brokers |
| `--max=N` | `0` | Max records to replay (`0` = all) |
| `--dry-run` | `false` | Count only; publish nothing |

It drains the topic and exits, committing offsets so a re-run won't replay the same records. Full
details: [`tools/dlt-replay-app`](tools/dlt-replay-app). ⚠️ Inspect first — if the bug still exists,
replayed records just fail again.

---

## ⚙️ Configuration

Every service is configured via environment variables (with sensible local defaults). The most common ones:

### Shared (all services)

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_PROFILES_ACTIVE` | `dev` | Active config profile (`dev` or `prod`) |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093` | Kafka broker list |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry URL |

### `accident-event-stream` (router)

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_TOPIC_NAME` | `accident.events` | Incoming incident topic |
| `EMERGENCY_SERVICE_TOPIC_NAME` | `emergency.events` | Emergency sink topic |
| `POLICE_SERVICE_TOPIC_NAME` | `law-enforcement.events` | Police sink topic |
| `FIRE_RESCUE_SERVICE_TOPIC_NAME` | `fire-rescue.events` | Fire-rescue sink topic |
| `STATISTICS_SERVICE_TOPIC_NAME` | `statistics.events` | Statistics sink topic |

### Consumer services (`emergency`, `law-enforcement`, `firerescue`, `statistics`)

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `18089` / `28089` / `38089` / `48089` | HTTP port (distinct per service so they can run on one host) |
| `MAIN_TOPIC_NAME` | e.g. `emergency.events` | The topic this service consumes |
| `DLT_TOPIC_NAME` | e.g. `emergency.events.dlt` | Dead-letter topic for unprocessable messages |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/accident_management_service` | JDBC URL |
| `POSTGRES_USER` | `test` | Database user (matches the bundled Docker infra) |
| `POSTGRES_PASSWORD` | `postgres` | Database password (matches the bundled Docker infra) |

> 🔐 **Production note:** never rely on the default credentials above. Supply secrets via environment variables, a secrets manager, or an orchestrator (see [`.env.example`](.env.example)).

### Profiles & health

Each service ships two Spring profiles — `dev` (default) and `prod`:

| | `dev` | `prod` |
|---|---|---|
| SQL logging | on | off |
| Hibernate `ddl-auto` | `update` | `validate` |
| Actuator health detail | `always` | `when-authorized` |
| Log level | `DEBUG` (app) | `WARN` (root) |

Select the profile with `SPRING_PROFILES_ACTIVE=prod`. Actuator endpoints are exposed at
`/actuator/health`, `/actuator/info`, and `/actuator/metrics`.

See each module's own `README.md` for its full parameter list.

---

## 🧪 Testing

```bash
# Run a module's tests
mvn -f services/emergency-service/pom.xml test
```

Integration tests use **Testcontainers** to spin up real Kafka, Schema Registry, and PostgreSQL instances — so Docker must be running.

---

## 📈 Observability

Every service exposes Spring Boot Actuator over HTTP — a Prometheus scrape endpoint at
`/actuator/prometheus` plus `/actuator/health` and `/actuator/metrics`. Metrics are tagged
with `application=<service-name>`.

The infra stack bundles Prometheus and Grafana:

```bash
docker compose up -d prometheus grafana
```

| Tool | URL | Notes |
|------|-----|-------|
| Prometheus | http://localhost:9090 | scrapes all services |
| Grafana | http://localhost:3000 | login `admin` / `admin`; the **AMS — Service Overview** dashboard is pre-provisioned |

> Prometheus reaches the host-run services via `host.docker.internal` on their default ports
> (`8080`, `18089`, `28089`, `38089`, `48089`) — run the services on those ports for metrics to appear.

---

## ☸️ Kubernetes

The apps are containerized with a single shared multi-stage [`Dockerfile`](Dockerfile), and
[`k8s/`](k8s) holds Kustomize manifests that deploy the services **and** a single-node dev
infrastructure (Kafka, Schema Registry, PostgreSQL) into an `ams` namespace.

```bash
# 1. build the images (one Dockerfile, selected per module)
docker build --build-arg MODULE=services/emergency-service -t ams/emergency-service .
#    … repeat for each service, plus tools/uiapp and tools/stream-bombarder-app

# 2. deploy everything
kubectl apply -k k8s/
kubectl -n ams get pods -w                       # wait until Ready

# 3. open the dashboard
kubectl -n ams port-forward svc/uiapp 9000:9000  # → http://localhost:9000
```

The router's init container pre-creates the Kafka topics at replication factor 1, so the
unchanged services run on a single broker. Full guide — building/loading images for
kind/minikube/Docker-Desktop, generating load, teardown — in [`k8s/README.md`](k8s/README.md).

---

## 📐 Architecture diagram

The full system architecture is maintained as an editable **[draw.io](https://www.drawio.com/)**
diagram — **[`Diagram.drawio`](Diagram.drawio)**. GitHub renders it when you open the file; you can
also view/edit it with [diagrams.net](https://app.diagrams.net/) or the *Draw.io Integration*
extension for VS Code / JetBrains.

---

