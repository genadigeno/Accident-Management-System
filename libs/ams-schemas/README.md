# ams-schemas — the AMS data contract

`ams-schemas` is the **single source of truth** for every message that flows through the
[Accident Management System](../../README.md). It holds the [Apache Avro](https://avro.apache.org/)
schemas (`.avsc`) for each Kafka message, and is published as a normal Maven library so that
**every service depends on the same generated Java classes** — a producer and a consumer can
never disagree about a message's shape.

At runtime, the [Confluent **Schema Registry**](https://docs.confluent.io/platform/current/schema-registry/index.html)
stores these schemas and hands out a numeric **schema id** for each one. Messages on Kafka carry
only that id (not the whole schema), so consumers always know exactly how to decode what they read.

---

## The contracts

Each schema maps to one Kafka topic. The router (`accident-event-stream`) reads the source event
and writes the per-responder events.

| Schema (`.avsc`) | Generated class | Topic | Produced by | Consumed by |
|------------------|-----------------|-------|-------------|-------------|
| `AccidentSchema.avsc` | `AccidentEventModel` | `accident.events` | stream-bombarder, uiapp | accident-event-stream, uiapp |
| `EmergencyAccident.avsc` | `EmergencyEventModel` | `emergency.events` | accident-event-stream | emergency-service |
| `PoliceAccident.avsc` | `PoliceEventModel` | `law-enforcement.events` | accident-event-stream | law-enforcement-service |
| `FireAccident.avsc` | `FireAccidentModel` | `fire-rescue.events` | accident-event-stream | firerescue-service |
| `StatisticalModel.avsc` | `StatisticalModel` | `statistics.events` | accident-event-stream | statistics-service |
| `DeserializationErrorResponse.avsc` | `DeserializationErrorResponse` | `accident.events.dlt` | accident-event-stream | (manual / replay) |

All classes are generated into the package **`ams.data.model`**. Shared types: the
`AccidentType` enum (`CRIMINAL`, `CAR_ACCIDENT`, `OTHER_ACCIDENT`, `FIRE_ACCIDENT`) and the
`Location` record (`address`, `latitude`, `longitude`).

> **Why "the message is the same for the services":** because all services compile against
> *this* jar, `EmergencyEventModel` is literally the same class everywhere. Change a field here,
> rebuild, and every service sees the change — that's the contract.

---

## How Avro + Schema Registry work (the important part)

A Kafka message value is **not** self-describing JSON. With the Confluent serializer the bytes are:

```
┌────────┬──────────────────┬─────────────────────────────┐
│ 0x00   │ schema id (4 B)  │ Avro-encoded payload        │
│ magic  │ big-endian int   │ (compact binary)            │
└────────┴──────────────────┴─────────────────────────────┘
```

The full schema lives in the registry, not on the topic. The flow:

```
PRODUCER  (KafkaAvroSerializer)                          CONSUMER  (KafkaAvroDeserializer)
─────────────────────────────────                        ─────────────────────────────────
serialize(EmergencyEventModel)                           read magic byte + schema id
   │  register/lookup schema under                          │  fetch schema by id  ◀── cached
   │  subject "emergency.events-value"                      │
   ▼            ┌────────────────────┐                      ▼
 get schema id ─┤  Schema Registry   ├───── schema by id ──▶ decode payload
   │            └────────────────────┘                      │  (specific.avro.reader=true →
   ▼                                                         ▼   maps to EmergencyEventModel)
 write [0x00 | id | payload] ───────── Kafka topic ────────▶ EmergencyEventModel object
```

- **Producers** register the schema on first send (auto-registration) and cache the id.
- **Consumers** fetch a schema by id once, then cache it — so the registry isn't hit per message.
- **Subjects:** with the default `TopicNameStrategy`, each topic gets a `…-value` (and `…-key`)
  subject, e.g. `emergency.events-value`. Keys in AMS are Avro strings → `emergency.events-key`
  holds the schema `"string"`.

---

## How a service uses it

### 1. Depend on the library

```xml
<dependency>
    <groupId>io.github.genadigeno</groupId>
    <artifactId>ams-schemas</artifactId>
    <version>2.3</version>
</dependency>
<!-- the Confluent Avro serializer + its repository are also required -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>6.2.0</version>
</dependency>
```
```xml
<repositories>
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>
```

### 2. Configure serializers + the registry URL

**Producer** (e.g. the router, stream-bombarder, uiapp):
```properties
spring.kafka.producer.key-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.properties.schema.registry.url=${SCHEMA_REGISTRY_URL:http://localhost:8081}
```

**Consumer** (the responder services):
```properties
spring.kafka.consumer.key-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.properties.specific.avro.reader=true
spring.kafka.properties.schema.registry.url=${SCHEMA_REGISTRY_URL:http://localhost:8081}
```

> `specific.avro.reader=true` is what makes the deserializer return the generated
> `EmergencyEventModel` class instead of a generic `GenericRecord`.

### 3. Use the generated class directly

```java
// producing
EmergencyEventModel event = ...;
kafkaTemplate.send("emergency.events", event.getCacheId().toString(), event);

// consuming
@KafkaListener(topics = "emergency.events")
public void handle(EmergencyEventModel event) { ... }
```

---

## Building & publishing the schemas

The `avro-maven-plugin` generates the Java classes from the `.avsc` files at the
`generate-sources` phase, so a normal build produces them.

```bash
# build + install to your local ~/.m2 so local services resolve it
mvn -f libs/ams-schemas/pom.xml clean install
```

Publishing to Maven Central is isolated in the `release` profile (GPG signing + OSSRH staging),
so ordinary builds never need a signing key:

```bash
mvn -f libs/ams-schemas/pom.xml -Prelease deploy
```

> Provide the GPG key/passphrase and OSSRH credentials via your Maven `settings.xml` /
> environment — never commit them.

---

## Changing a schema

1. Edit the `.avsc` file (or add a new one) under `src/main/avro/`.
2. **Bump the version** in `libs/ams-schemas/pom.xml` (e.g. `2.3` → `2.4`).
3. `mvn -f libs/ams-schemas/pom.xml clean install`.
4. Update the `<version>` of the `ams-schemas` dependency in every module that uses the changed schema.
5. Rebuild the affected services.

**Compatibility:** the registry enforces a compatibility mode per subject (default `BACKWARD`).
To stay backward-compatible, only make additive changes — **add fields with a `default`**, don't
remove or rename existing fields, and don't change their types. Check before publishing:

```bash
# requires the kafka-schema-registry-maven-plugin to be configured
mvn io.confluent:kafka-schema-registry-maven-plugin:test-compatibility
```

---

## Inspecting the registry

With the Schema Registry running (`http://localhost:8081`):

```bash
# list all subjects
curl -s http://localhost:8081/subjects

# the latest schema registered for a topic's value
curl -s http://localhost:8081/subjects/emergency.events-value/versions/latest

# fetch a schema by its global id
curl -s http://localhost:8081/schemas/ids/1

# the compatibility mode for a subject
curl -s http://localhost:8081/config/emergency.events-value
```

---

## Schema reference

<details>
<summary><code>AccidentEventModel</code> — the source event (<code>accident.events</code>)</summary>

| Field | Avro type | Notes |
|-------|-----------|-------|
| `id` | `long` | |
| `type` | `enum AccidentType` | `CRIMINAL` · `CAR_ACCIDENT` · `OTHER_ACCIDENT` · `FIRE_ACCIDENT` |
| `date` | `int` (logicalType `date`) | → `java.time.LocalDate` |
| `location` | `record Location` | `address`, `latitude`, `longitude` (all `string`) |
| `description` | `string` | |
| `cacheId` | `string` | used as the Kafka message key |
</details>

<details>
<summary><code>EmergencyEventModel</code> / <code>PoliceEventModel</code> / <code>FireAccidentModel</code> — responder events</summary>

| Field | Avro type |
|-------|-----------|
| `id` | `long` |
| `date` | `int` (logicalType `date`) |
| `latitude` | `string` |
| `address` | `string` |
| `longitude` | `string` |
| `description` | `string` |
| `cacheId` | `string` |
</details>

<details>
<summary><code>StatisticalModel</code> — windowed aggregate (<code>statistics.events</code>)</summary>

| Field | Avro type | Notes |
|-------|-----------|-------|
| `id` | `long` | |
| `type` | `enum AccidentType` | |
| `from` | `long` (logicalType `local-timestamp-millis`) | window start |
| `end` | `long` (logicalType `local-timestamp-millis`) | window end |
| `count` | `long` | number of events in the window |
| `cacheId` | `string` | |
</details>

<details>
<summary><code>DeserializationErrorResponse</code> — streams dead-letter record</summary>

| Field | Avro type |
|-------|-----------|
| `date` | `int` (logicalType `date`) |
| `value` | `string` |
| `description` | `string` |
| `key` | `string` |
</details>
