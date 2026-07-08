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
| PostgreSQL | `localhost:5432`, user `test`, password `postgres`; auto-creates a database **per service** on first start (`ams_emergency`, `ams_lawenf`, `ams_firerescue`, `ams_statistics`, `ams_dispatch`, `ams_notification`, `ams_correlation`) — see [`db/`](db) |
| Prometheus | `http://localhost:9090` — scrapes the host-run services' `/actuator/prometheus` (config in [`monitoring/`](monitoring)) |
| Grafana | `http://localhost:3000` — login `admin` / `admin`; **AMS — Service Overview** dashboard pre-provisioned |

> **Elasticsearch is not part of this stack** — `search-service` needs one separately at
> `localhost:9200` (security disabled). Run your own single node or cluster.

Tear it down with `docker compose down` (add `-v` to also remove volumes; wiping the volume also
drops the per-service databases, which are re-created on the next `up`).

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

> These are Windows `.bat` scripts covering the router + original four responders. The newer
> services (`dispatch`, `notification`, `incident-correlation`, `citizen-report-gateway`,
> `search`, `enrichment`) have no `.bat` helper — start them with the `java -jar` commands (and
> per-service `POSTGRES_URL`) from the [root README](../README.md#-installation). On Unix/macOS use
> those commands directly.

## `monitoring/`

Prometheus scrape config and Grafana provisioning (datasource + the AMS dashboard), mounted
into the Prometheus and Grafana containers by the root `docker-compose.yml`.

## `db/`

`db/init/` holds the Postgres bootstrap SQL that **creates the per-service databases**
(`01-create-databases.sql`). The root `docker-compose.yml` mounts it into the container's
`/docker-entrypoint-initdb.d`, so the databases are created automatically the first time the
`ams-db` volume initialises. (Kubernetes does the equivalent via
[`k8s/postgres-init.configmap.yaml`](../k8s/postgres-init.configmap.yaml).)
