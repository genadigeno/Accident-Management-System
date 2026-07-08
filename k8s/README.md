# AMS on Kubernetes

Kustomize manifests to run the whole Accident Management System on a Kubernetes cluster —
the application services **and** a single-node dev infrastructure (Kafka, Schema Registry,
PostgreSQL).

> ⚠️ **Dev / demo only.** The infra here runs single replicas with replication factor 1 and no
> persistence guarantees. For production, run Kafka via an operator (e.g. **Strimzi**), use a
> managed/operator PostgreSQL, and supply secrets from a real secret store.

## What's deployed

| Kind | Names |
|------|-------|
| Infra | `kafka` (KRaft StatefulSet), `schema-registry`, `postgres` (StatefulSet, one DB per consumer) |
| Services | `accident-event-stream` (router), `emergency-service`, `law-enforcement-service`, `firerescue-service`, `statistics-service`, `dispatch-service`, `notification-service`, `incident-correlation-service`, `citizen-report-gateway`, `search-service`, `enrichment-service` |
| Dashboard | `uiapp` |
| On-demand | `stream-bombarder` (a `Job`, applied separately) |

> **Not deployed here:** Elasticsearch. `search-service` reads `ELASTICSEARCH_URL`
> (default `http://elasticsearch:9200`) — provide your own, or scale that Deployment to 0
> (`kubectl -n ams scale deploy/search-service --replicas=0`) if you don't need search.

Everything lands in the **`ams`** namespace. Apps read Kafka/Schema-Registry from the `ams-config`
ConfigMap and DB credentials from the `ams-db` Secret.

## Prerequisites

- A Kubernetes cluster — [kind](https://kind.sigs.k8s.io/), [minikube](https://minikube.sigs.k8s.io/),
  or Docker Desktop's built-in Kubernetes.
- `kubectl` (Kustomize is built in — no separate install).
- Docker, to build the images.

## 1. Build the images

All apps share one [`Dockerfile`](../Dockerfile) (multi-stage; selected via `MODULE`). Build every
image with the helper script:

```bash
./k8s/build-images.sh            # macOS / Linux / Git Bash
```
```powershell
.\k8s\build-images.ps1           # Windows PowerShell
```

Or build a single image directly:

```bash
docker build --build-arg MODULE=tools/uiapp -t ams/uiapp:latest .
```

> Behind a TLS-intercepting proxy, append
> `--build-arg MAVEN_CLI_OPTS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true"`
> — the scripts forward any extra args to each `docker build`.

## 2. Make the images available to the cluster

The manifests use `imagePullPolicy: IfNotPresent` with local `ams/*` tags (no registry).

- **Docker Desktop K8s:** nothing to do — it shares the local Docker images.
- **kind:** `for n in accident-event-stream emergency-service law-enforcement-service firerescue-service statistics-service dispatch-service notification-service incident-correlation-service citizen-report-gateway search-service enrichment-service uiapp stream-bombarder; do kind load docker-image ams/$n:latest; done`
- **minikube:** `for n in …; do minikube image load ams/$n:latest; done`

To use a registry instead, push the images and set the tags in
[`kustomization.yaml`](kustomization.yaml) under `images:` (e.g. `newName: ghcr.io/you/ams-uiapp`).

## 3. Deploy

```bash
kubectl apply -k k8s/
kubectl -n ams get pods -w        # wait until all are Running/Ready
```

Startup order is self-healing: the router's init container waits for Kafka and pre-creates the
topics; consumers retry until their topics and database exist.

## 4. Use it

Open the dashboard:

```bash
kubectl -n ams port-forward svc/uiapp 9000:9000
# → http://localhost:9000
```

Generate load (the bombarder Job is not part of the always-on stack):

```bash
kubectl -n ams delete job stream-bombarder --ignore-not-found
kubectl -n ams apply -f k8s/stream-bombarder.job.yaml
kubectl -n ams logs -f job/stream-bombarder
```

Inspect a consumer's database:

```bash
kubectl -n ams exec -it postgres-0 -- psql -U test -d ams_emergency -c \
  "SELECT count(*) FROM emergency_accidents;"
```

## 5. Tear down

```bash
kubectl delete -k k8s/            # keeps PVCs
kubectl -n ams delete pvc --all   # also wipe Kafka/Postgres data
```

## How single-node Kafka works here

The services declare their topics with `replicas(3)`. Rather than change the app, the router's
**`create-topics` init container** pre-creates every topic at **RF 1** with `min.insync.replicas=1`
before the app starts — so Spring's `KafkaAdmin` finds them already present and doesn't try to
re-create them at RF 3 (which would fail on one broker). Kafka Streams' own internal topics inherit
the broker default RF (1).

## Notes

- **One database per consumer.** `postgres-init` creates `ams_emergency`, `ams_lawenf`,
  `ams_firerescue`, `ams_statistics`, `ams_dispatch`, `ams_notification`, `ams_correlation`; each
  service's `POSTGRES_URL` points at its own (they can't share one schema — their Flyway
  migrations would collide). `citizen-report-gateway`, `search-service` and `enrichment-service`
  have no database.
- **Probes.** Services use the actuator liveness/readiness groups; the `uiapp` (no actuator) is
  probed on `/`.
- **Metrics.** Pods carry `prometheus.io/scrape` annotations; point a Prometheus (e.g. the
  Prometheus Operator's pod-annotation scrape) at the `ams` namespace.
