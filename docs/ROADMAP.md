# AMS Feature Roadmap

Ideas and planned features for the [Accident Management System](../README.md), grounded in how
real-world computer-aided dispatch (CAD / 911) systems work. This is the successor to the
original per-service `NEW_FEATURES` specs (now under [`infra/`](../infra)) — those first-wave
features (routing, geo-fencing, fraud detection, BOLO, hospital lookup, building plans,
statistics API) are all **implemented**.

## The gap, seen through a real CAD lens

A real dispatch system runs a five-stage lifecycle: **intake → classify/prioritize → assign
units → track status to closure → analyze**
([SmartCOP](https://www.smartcop.com/understanding-cad-dispatch-software/),
[Wikipedia: CAD](https://en.wikipedia.org/wiki/Computer-aided_dispatch)). AMS covers intake
(simulated), routing, recording, and analytics — the roadmap adds prioritization, unit
dispatching, a status lifecycle, notifications, and duplicate-report merging (a real 911
problem — see [Axon 911](https://www.axon.com/solutions/axon-911) and
[PSAP duplicate-call handling](https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/10742809)).

---

## Phases

| Phase | Scope | Effort | Status |
|:-----:|-------|:------:|:------:|
| **1** | **`dispatch-service`** — unit registry, nearest-available assignment, `unit.status.events` lifecycle (`DISPATCHED → EN_ROUTE → ON_SCENE → CLEARED`), built-in movement simulator, call stacking when no unit is free | L | ✅ done |
| **2** | Emergency **response-time tracking + SLA alerts** (consumes `unit.status.events`); statistics **hourly/daily/weekly rollups, trend %, CSV report** | M | ✅ done |
| **3** | **`bolo.alerts` topic** + **`notification-service`** (fan-in from bolo/fraud/sensitive/SLA topics → log + webhook channels first, SMTP/Telegram behind adapters; dedup + rate limits) | M | ✅ done |
| **4** | **`incident-correlation-service`** (geohash cell + time window merges duplicate reports into one incident, `incident.events` lifecycle) + **`citizen-report-gateway`** (public intake API with validation, rate limiting, duplicate-hint) | L | ⬜ |
| **5** | **`search-service`** on Elasticsearch (index all responder events; full-text + geo + time queries; past-incident correlation API; Kibana) + uiapp **map view & alert panel** | M | ⬜ |
| **6** | **`enrichment-service`** (weather via Open-Meteo, district via reverse geocoding), fire **hydrant lookup** (Overpass), **real building-plan DB**, crime **hotspot API**, ML hotspot baseline (EWMA/Poisson) | M–L | ⬜ |

---

## A. Features for existing services

### Router (`accident-event-stream`)
- **Priority classification (P1–P4)** from type + description keywords, as an additive Avro field
  with a default — real dispatch triages every call
  ([AEDR: police dispatch priority levels](https://www.aedrjournal.org/the-distribution-of-emergency-police-dispatch-call-incident-types-and-priority-levels-within-the-police-priority-dispatch-system)).
- Consume `accident.events.enriched` (Phase 6) when present — same topology, richer payloads.

### Law-enforcement
- **`bolo.alerts` topic** (Phase 3): today a BOLO ends as a DB row + log line; publishing
  HIGH/CRITICAL alerts lets the notification service and dashboard react in real time.
- **Crime hotspot API**: counts grouped by geohash cell + hour-of-day (pure SQL) —
  "most thefts happen 2–4 AM in District X" from the original spec.
- **Case lifecycle**: `OPEN/ASSIGNED/CLOSED` status + `PATCH /api/v1/cases/{id}` — the
  records-management closure stage CAD requires.

### Emergency
- **Response-time tracking + SLA** (Phase 2, original spec TODO): consume `unit.status.events`,
  compute dispatch→on-scene, persist, `GET /api/v1/response-times?district=`, alert when > 15 min.
- **Hospital capacity registry**: beds available per hospital; `nearby` ranks by distance *and*
  capacity.
- **Medical triage keywords** ("unconscious", "not breathing") → triage level, mirroring BOLO.

### Fire-rescue
- **Real building-plan store**: Postgres-backed provider + import endpoint; stub as fallback.
- **Hydrant lookup** via Overpass `emergency=fire_hydrant` — same adapter/cache pattern as hospitals.
- **Hazmat flag** from description keywords → flag + metric.

### Statistics
- **Rollups + trends** (Phase 2, spec TODO): `/api/v1/stats/hourly|daily|weekly`, trend endpoint
  with % change vs the previous period, peak-hours.
- **Report generation** (spec TODO): CSV endpoint first, PDF (OpenPDF) later; optional scheduled
  export. Folded into this service — no new microservice needed.

### uiapp
- **Map view** (Leaflet; lat/lng already in every feed DTO), **alert panel**
  (bolo/fraud/sensitive), **incident drill-down** into the owning service's API.

---

## B. New microservices

1. **`dispatch-service`** *(Phase 1)* — the missing CAD core. Registry of response units
   (ambulances, patrol cars, fire engines) with state and location; consumes the three responder
   topics; assigns the **nearest available unit** (Haversine); emits `unit.status.events`
   (`DISPATCHED → EN_ROUTE → ON_SCENE → CLEARED`) with a built-in simulator advancing statuses
   over randomized realistic durations; **call stacking** (waiting queue) when no unit is free.
   Own PostgreSQL DB; APIs `/api/v1/units`, `/api/v1/dispatches`.
2. **`notification-service`** *(Phase 3)* — consumes all alert-class topics, applies dedup and
   per-rule rate limits, fans out to channels (log + webhook first; SMTP/Telegram adapters).
3. **`incident-correlation-service`** *(Phase 4)* — merges duplicate reports of the same
   real-world incident (geohash ~150 m + 10-min window) into an `incident.events` lifecycle with
   `reportCount`; the constructive sibling of the fraud detector.
4. **`search-service`** *(Phase 5)* — indexes every responder event into Elasticsearch
   (an ELK stack is already available); full-text + `geo_distance` + time-range queries; enables
   past-incident correlation ("3rd robbery at this bank in 2 months") and Kibana dashboards.
5. **`enrichment-service`** *(Phase 6)* — `accident.events → accident.events.enriched`: weather
   at the location (Open-Meteo, free, no API key), district via reverse geocoding, past-incident
   count via search-service. Additive Avro fields with defaults.
6. **`citizen-report-gateway`** *(Phase 4)* — public `POST /api/v1/reports` with validation,
   per-source rate limiting, API keys, and a correlation-powered duplicate hint ("already
   reported: incident …, 3 reports").
7. **`ml-prediction-service`** *(stretch)* — hotspot forecasting per cell/hour; start with an
   EWMA/Poisson baseline, upgrade path per the
   [incident-prediction survey](https://arxiv.org/pdf/2006.04200).

---

## C. Platform track (parallel)

- OpenTelemetry tracing across Kafka hops.
- Schema-compatibility check in CI (`kafka-schema-registry-maven-plugin`).
- Testcontainers end-to-end suite; GitHub Actions image publishing.
- k8s: HPA, PodDisruptionBudgets; Kafka Connect sinks (Elasticsearch, S3 archive).

## Sources

[SmartCOP — CAD core functions](https://www.smartcop.com/understanding-cad-dispatch-software/) ·
[Wikipedia — Computer-aided dispatch](https://en.wikipedia.org/wiki/Computer-aided_dispatch) ·
[GINA — CAD explained](https://www.ginasoftware.com/blog/computer-aided-dispatch/) ·
[Axon 911](https://www.axon.com/solutions/axon-911) ·
[USPTO — duplicate-call handling at PSAPs](https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/10742809) ·
[AEDR — police dispatch priority levels](https://www.aedrjournal.org/the-distribution-of-emergency-police-dispatch-call-incident-types-and-priority-levels-within-the-police-priority-dispatch-system) ·
[arXiv — incident prediction & dispatch models survey](https://arxiv.org/pdf/2006.04200)
