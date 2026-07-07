# Capacity & Sizing

How much this project can handle, and how to size it. The dashboard shows a live version of
this (the **System capacity** card, computed by `GET /api/v1/system` from the host it runs on);
this document is the durable record plus the load-test evidence behind the recommendations.

## Measured host (development machine)

| Resource | Value |
|----------|-------|
| CPU | Intel i7-8700 — **6 cores / 12 threads** @ 3.2 GHz |
| Physical RAM | **31.9 GB** |
| Docker Desktop VM (WSL2) | **15.6 GB** allocated |
| OS | Windows 10, amd64 |

The JVM services run **on the host** (`java -jar`); the infrastructure (Kafka ×3, Schema
Registry, PostgreSQL, Prometheus, Grafana, Elasticsearch ×3) runs **in the Docker VM**. So the
two pools of RAM are largely separate: ~15.6 GB for infra, the rest of the 31.9 GB for the OS +
the JVM services.

## Approximate footprints

| Component | RAM |
|-----------|-----|
| Each AMS Spring Boot service (host) | ~350–500 MB resident |
| Kafka broker ×3 | ~1.2 GB each (~3.7 GB) |
| Elasticsearch node ×3 | ~1.25 GB each (~3.75 GB) |
| Schema Registry | ~0.5 GB |
| PostgreSQL (256 MB shared_buffers) | ~0.3–0.5 GB |
| Prometheus + Grafana | ~0.2 GB |

Infra without ES ≈ **5 GB**; with the ES cluster ≈ **8.5 GB** — both fit inside the 15.6 GB VM.

## Recommendations

**Instances (RAM-bound).** Reserving ~half the RAM for infrastructure + OS and budgeting
~450 MB per service leaves room for roughly **35 service instances** on this host. The full
project (12 JVM apps) uses only ~3.5 GB, so there is ample RAM headroom.

**Throughput (CPU-bound).** With only 12 logical threads, CPU — not RAM — is the ceiling for a
data-intensive, exactly-once pipeline. Keep the number of **actively-consuming threads**
(listeners × their `concurrency`) in the neighbourhood of the core count; heavy oversubscription
just adds context-switching. Rule of thumb: **~1 busy consumer thread per logical core** (≈12
here), and scale the *bottleneck* consumer horizontally rather than adding unrelated services.

**Sustained event throughput (measured on this host):**

| Metric | Result |
|--------|--------|
| Produced to the source topic | ~2,500 events/sec |
| End-to-end through the full fan-out (router → responders persisted) | ~420 ev/s at `concurrency=1`, ~950 ev/s at `concurrency=3` |
| Bottleneck consumer (law-enforcement, 100% of events) | ~505 → ~1,040 rows/sec (1→3 concurrency) |
| Total DB writes across services under load | ~1,500 rows/sec |

So a realistic **sustained** end-to-end rate on this machine is roughly **1,000–2,000 events/sec**
with the whole fan-out active; short bursts go higher (the producer alone does ~2,500/sec). The
exactly-once Kafka Streams router is the end-to-end ceiling — raising throughput further means
more router stream threads / partitions, or scaling the heaviest consumers.

**Don't run** more than one Elasticsearch cluster, and avoid running the ES cluster *and* a large
load test *and* every service at once on a 15.6 GB VM — that was what previously triggered OOM
kills. Free the ES cluster (`docker stop es01 es02 es03`) when not exercising search.

## How the live card computes it

`SystemInfoController` reads `com.sun.management.OperatingSystemMXBean` (CPU threads, total/free
RAM, CPU load) and `Runtime` (JVM max heap) from whatever host/container the dashboard runs on,
then applies the heuristics above. In Kubernetes it reflects the pod's limits, so the same card
gives environment-appropriate numbers wherever it is deployed.
