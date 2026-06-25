# Infrastructure helpers

Supporting files for the [Accident Management System (AMS)](../README.md): the monitoring
configuration and convenience scripts for launching the services.

> The tools/infrastructure (Kafka, Schema Registry, PostgreSQL, Prometheus, Grafana) are defined
> in the **root [`docker-compose.yml`](../docker-compose.yml)** — the application services run on
> the host with `java -jar`, not in Docker.

## Start the tools

```bash
docker compose up -d            # from the repo root
docker compose ps               # wait until healthy
```

This brings up:

| Component | Details |
|-----------|---------|
| Kafka brokers | 3 brokers (`kafka1`/`kafka2`/`kafka3`) in **KRaft** mode (no ZooKeeper), host ports `9091`/`9092`/`9093` |
| Schema Registry | `http://localhost:8081` |
| PostgreSQL | `localhost:5432`, database `accident_management_service`, user `test`, password `postgres` |
| Prometheus | `http://localhost:9090` — scrapes the host-run services' `/actuator/prometheus` (config in [`monitoring/`](monitoring)) |
| Grafana | `http://localhost:3000` — login `admin` / `admin`; **AMS — Service Overview** dashboard pre-provisioned |

Tear it down with `docker compose down` (add `-v` to also remove volumes).

## Helper run scripts

After building the services (see the [root README](../README.md#-installation)), these scripts
start a service with sensible local defaults. Each launch starts a **new instance** on a
random/distinct port.

| Script | Service |
|--------|---------|
| `run-ams-event-stream.bat` | Router (`accident-event-stream`) |
| `run-emergency-service-app.bat` | Emergency service |
| `run-fire-rescue-service-app.bat` | Fire-rescue service |
| `run-law-enforcement-service-app.bat` | Law-enforcement service |
| `run-statistics-service-app.bat` | Statistics service |
| `start-stream-bombarder.bat` | Load generator (`stream-bombarder-app`) |

> These are Windows `.bat` scripts. On Unix/macOS, use the `java -jar` commands from the
> [root README](../README.md#-installation).

## `monitoring/`

Prometheus scrape config and Grafana provisioning (datasource + the AMS dashboard), mounted
into the Prometheus and Grafana containers by the root `docker-compose.yml`.

## `db/`

Optional SQL bootstrap scripts that can be mounted into the PostgreSQL container
(see the commented `volumes:` block in the root `docker-compose.yml`).
