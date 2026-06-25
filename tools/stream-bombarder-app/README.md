# Stream Bombarder

A command-line **load generator** for the [Accident Management System (AMS)](../../README.md).
It floods the source topic (`accident.events`) with bursts of random, Avro-serialized accident
events so you can load-test the pipeline (routing, persistence, analytics) and watch it under
pressure.

- Events are sent **transactionally** with an idempotent producer, keyed by a random `cacheId`.
- It runs **until you stop it** (Ctrl-C), or until a `--count` / `--duration-sec` limit is hit.
- It is designed to be run as **multiple parallel instances** — each is an independent producer
  with its own transactional id, so you can stack load by launching several.

## Prerequisites

- **Java 17+** (`java -version`)
- A running **Kafka cluster + Schema Registry** — see the [root README](../../README.md#-installation).
- **Maven** (only to build) — `mvn -version`

## Build

From the repository root (this also builds the shared `ams-schemas` dependency):

```bash
mvn -pl tools/stream-bombarder-app -am clean package
```

This produces a self-contained runnable jar at:

```
tools/stream-bombarder-app/target/stream-bombarder.jar
```

## Run

The same jar runs everywhere; only the shell syntax differs.

### macOS / Linux (bash, zsh)

```bash
java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --scale=5
```

### Windows (PowerShell)

```powershell
java -jar tools\stream-bombarder-app\target\stream-bombarder.jar --scale=5
```

### Windows (Command Prompt)

```bat
java -jar tools\stream-bombarder-app\target\stream-bombarder.jar --scale=5
```

Press **Ctrl-C** to stop; it finishes the current burst, closes the producer, and prints the
grand total.

## Options

Every setting can be supplied three ways (highest precedence first): a **CLI flag**, an
**environment variable**, or a key in a **`.env`** file in the working directory.

| CLI flag | Env var | `.env` key | Default | Description |
|----------|---------|------------|---------|-------------|
| `--bootstrap-servers=…` | `BOOTSTRAP_SERVERS` | `bootstrap-servers` | `localhost:9092,localhost:9093` | Kafka brokers |
| `--schema-registry-url=…` | `SCHEMA_REGISTRY_URL` | `schema-registry-url` | `http://localhost:8081` | Confluent Schema Registry |
| `--topic=…` | `TOPIC` | `topic` | `accident.events` | Target topic |
| `--scale=N` | `SCALE` | `scale` | `1` | Burst-size multiplier |
| `--max-burst=N` | `MAX_BURST` | `max-burst` | `100` | Max events per burst (before scale) |
| `--interval-ms=N` | `INTERVAL_MS` | `interval-ms` | `1000` | Max delay between bursts (random 0–N ms) |
| `--count=N` | `COUNT` | `count` | `0` | Stop after N events (`0` = unlimited) |
| `--duration-sec=N` | `DURATION_SEC` | `duration-sec` | `0` | Stop after N seconds (`0` = unlimited) |
| `--help`, `-h` | — | — | — | Print usage and exit |

Each burst sends a random `1 … (max-burst × scale)` events. To configure via file, copy
[`.env.example`](.env.example) to `.env` and edit it (`.env` is git-ignored).

## Examples

Bounded smoke test — exactly 10,000 events then exit:

```bash
java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --count=10000 --max-burst=200
```

Run for 60 seconds at higher load:

```bash
java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --scale=10 --duration-sec=60
```

Point at a remote cluster via environment variables (macOS/Linux):

```bash
BOOTSTRAP_SERVERS=broker:9092 SCHEMA_REGISTRY_URL=http://sr:8081 \
  java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --scale=3
```

Windows PowerShell equivalent:

```powershell
$env:BOOTSTRAP_SERVERS="broker:9092"; $env:SCHEMA_REGISTRY_URL="http://sr:8081"
java -jar tools\stream-bombarder-app\target\stream-bombarder.jar --scale=3
```

## Running multiple instances

Launch several producers to multiply load. Each gets a unique transactional id automatically.

**macOS / Linux** — background several and stop them all with Ctrl-C:

```bash
for i in 1 2 3 4; do
  java -jar tools/stream-bombarder-app/target/stream-bombarder.jar --scale=5 &
done
wait
```

**Windows (PowerShell)** — separate windows:

```powershell
1..4 | ForEach-Object {
  Start-Process java -ArgumentList '-jar','tools\stream-bombarder-app\target\stream-bombarder.jar','--scale=5'
}
```

## Output

With a clean, timestamped console log (Kafka's own chatter is kept to warnings):

```
2026-06-25 01:10:33.512 [INFO] Application - stream-bombarder starting | bootstrap=localhost:9092,localhost:9093 schema-registry=http://localhost:8081 topic=accident.events scale=5 max-burst=100 interval-ms=1000 count=unlimited duration-sec=unlimited
2026-06-25 01:10:34.090 [INFO] Application - sent 213 event(s) | 213 total
2026-06-25 01:10:34.640 [INFO] Application - sent 47 event(s) | 260 total
^C
2026-06-25 01:10:36.110 [INFO] Application - shutdown requested, finishing current burst...
2026-06-25 01:10:36.180 [INFO] Application - stopped | 412 event(s) sent in total
```

To see the effect downstream, watch the router/responder logs, the
[Grafana dashboard](../../README.md#-observability), or the [uiapp](../uiapp) live dashboard.

> ⚠️ `--scale`, `--max-burst`, and multiple instances multiply load fast — this can produce
> **millions** of messages and saturate CPU/disk. Start small (`--scale=1`–`10`).

## Tech stack

Java 17 · Apache Kafka client (transactional, idempotent) · Apache Avro + Confluent Schema
Registry (via [`ams-schemas`](../../libs/ams-schemas)) · SLF4J Simple · packaged as a shaded jar.
