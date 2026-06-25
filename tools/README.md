# Tools

Utility applications for operating and exercising the
[Accident Management System](../README.md). None of them are part of the runtime pipeline —
they help you generate load, recover failed messages, and watch the system live.

| Tool | What it does | Output / what it gives you |
|------|--------------|----------------------------|
| [`stream-bombarder-app`](stream-bombarder-app) | Floods the source topic with random accident events (load testing). | Console logs of per-burst and total events sent. |
| [`dlt-replay-app`](dlt-replay-app) | Reads a dead-letter topic and republishes the records to a target topic after a fix. | Console logs of how many records were replayed (supports `--dry-run`). |
| [`uiapp`](uiapp) | A live web dashboard. | Browser UI at `http://localhost:8080`: live event feed, per-type counters, and an event generator. |

## Build

All three build with the root aggregator (`mvn clean package`), or individually:

```bash
mvn -f tools/stream-bombarder-app/pom.xml clean package   # -> target/stream-bombarder.jar
mvn -f tools/dlt-replay-app/pom.xml        clean package   # -> target/dlt-replay.jar
mvn -f tools/uiapp/pom.xml                 clean package   # -> target/uiapp-*.jar
```

The bombarder and the replay tool build **runnable fat jars** (`java -jar …`); `uiapp` is a
Spring Boot app. See each tool's own README for full install and usage details.
