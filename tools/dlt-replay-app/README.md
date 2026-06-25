# DLT Replay Tool

Part of the [Accident Management System (AMS)](../../README.md). A small standalone tool that
reads records from a **dead-letter topic** and republishes their original key/value bytes to a
**target topic** — so messages that failed can be re-processed once the underlying issue is fixed.

Spring's internal `DLT_*` and deserialization-exception headers are stripped on replay so the
record re-enters the pipeline cleanly. Records are replayed as-is (byte-for-byte), so the format
(Avro) is preserved.

## Build

```bash
mvn -f tools/dlt-replay-app/pom.xml clean package
```

Produces a runnable fat jar at `target/dlt-replay.jar`.

## Usage

```bash
java -jar tools/dlt-replay-app/target/dlt-replay.jar \
    --bootstrap.servers=localhost:9092,localhost:9093 \
    --source.topic=emergency.events.dlt \
    --target.topic=emergency.events
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--bootstrap.servers` | `localhost:9092,localhost:9093` | Kafka brokers |
| `--source.topic` | _(required)_ | The dead-letter topic to read from |
| `--target.topic` | _(required)_ | The topic to republish to |
| `--group.id` | `dlt-replay-<random>` | Consumer group id |
| `--max` | `0` | Max records to replay (`0` = everything currently on the topic) |
| `--dry-run` | `false` | Count only; publish nothing |

The tool reads from the earliest offset, replays until the topic is drained (or `--max` is
reached), commits its offsets, and exits — so re-running it won't replay the same records again
(use a fresh `--group.id` to replay from the beginning).

> ⚠️ Inspect before replaying. If records are in the DLT because of a bug that still exists, they
> will just fail again. Use `--dry-run` first to see how many are there.
