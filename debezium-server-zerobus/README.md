# Debezium Server ZeroBus Sink

The ZeroBus sink is a native Debezium Server sink selected with:

```properties
debezium.sink.type=zerobus
```

The native sink path is:

```text
Debezium Server -> debezium-server-zerobus -> ZeroBus native ingest -> Unity Catalog table
```

This module intentionally does not run the Kafka Connect worker/runtime, Kafka brokers, Kafka producer
APIs, or Kafka Connect SMTs. It may inspect Debezium's embedded `SourceRecord` data model when needed,
as other Debezium Server table-oriented sinks do.

The first native implementation uses the Databricks ZeroBus Java SDK with JSON, protobuf, and Arrow
streams. Each Debezium batch is serialized in the configured format, written with the SDK offset
API, and acknowledged with `waitForOffset(...)` before Debezium source offsets are committed.

The review packet for the first upstream contribution is
`debezium-server-zerobus/UPSTREAM_CONTRIBUTION_PACKET.md`.

## Configuration

```properties
debezium.sink.zerobus.endpoint=https://zerobus.example
debezium.sink.zerobus.workspace.url=https://dbc-a1b2c3d4-e5f6.cloud.databricks.com
debezium.sink.zerobus.authentication.type=oauth2
debezium.sink.zerobus.authentication.oauth2.client-id=${ZEROBUS_CLIENT_ID}
debezium.sink.zerobus.authentication.oauth2.client-secret=${ZEROBUS_CLIENT_SECRET}
debezium.sink.zerobus.record.format=json
debezium.sink.zerobus.max.inflight.batches=1000
debezium.sink.zerobus.idempotency.mode=source
debezium.sink.zerobus.tombstone.handling.mode=event
debezium.sink.zerobus.table.mapping.mode=source
debezium.sink.zerobus.table.mapping.default.catalog=main
debezium.sink.zerobus.table.mapping.default.schema=bronze
```

`workspace.url` is the Databricks workspace URL used by the SDK as the Unity Catalog endpoint.
The OAuth client id and secret should come from a Databricks service principal with `USE CATALOG`,
`USE SCHEMA`, `SELECT`, and `MODIFY` on the target table.

`record.format` supports `json`, `protobuf`, and `arrow`. All formats use the same stable ZeroBus envelope:
`target_table`, `destination`, `partition`, `operation`, `idempotency_key`, `key`, `value`,
`source_position`, and `headers`.

In JSON mode, Debezium JSON strings are embedded as JSON objects rather than double-quoted text. In
protobuf mode, the sink creates a `DebeziumZeroBusRecord` descriptor and writes the same envelope
through `ZerobusProtoStream`; key/value/maps are encoded as canonical JSON strings inside the
protobuf envelope. In Arrow mode, the sink creates the same envelope as an Arrow `Schema`, writes a
`VectorSchemaRoot` through `ZerobusArrowStream`, and keeps key/value/maps as canonical JSON strings
in nullable UTF-8 columns. Table-specific typed protobuf or source-table-shaped Arrow rows remain a
future schema-evolution track.

`max.inflight.batches` configures the ZeroBus SDK Arrow stream's in-flight batch limit. It is
separate from `max.inflight.records`, which applies to JSON/protobuf SDK streams.

`operation` is inferred from Debezium's envelope `op` code when available: `c`, `r`, `u`, and `d`
become `create`, `read`, `update`, and `delete`. Null-value Debezium tombstone records are distinct
from delete events and are emitted as `operation=tombstone` unless `tombstone.handling.mode=drop`.

`idempotency.mode=source` writes a deterministic envelope-level `idempotency_key` from Debezium's destination,
partition, key, and the underlying `SourceRecord` source partition/offset when available. This keeps
LSN/transaction ordering metadata with the ZeroBus row. Header fingerprints are used only as a
fallback for non-embedded events. Use `idempotency.mode=none` only when the target table or
downstream process supplies its own dedupe contract.

## Table Mapping

The sink supports three routing modes:

- `source`: maps the Debezium destination's last segment to
  `<default.catalog>.<default.schema>.<table>`.
- `explicit`: maps exact Debezium destinations with
  `debezium.sink.zerobus.table.mapping.overrides`.
- `regex`: applies a native regex/replacement pair to the Debezium destination.

All target tables must be Unity Catalog identifiers in the form:

```text
catalog.schema.table
```

## Commit Semantics

The sink maps and writes records first, waits for a durable ZeroBus acknowledgement, and only then
marks records processed with Debezium's committer. A write failure or partial acknowledgement fails
the batch before offsets are advanced. This keeps the first native implementation at-least-once
until service-side idempotent append or row-level dedupe semantics are explicitly defined.

The writer preserves Debezium input order across interleaved target tables while still batching
consecutive records for the same target table. This keeps source-position ordering visible in the
write sequence instead of regrouping a mixed-source batch by target table.

## Java 25 Local Verification

For local ZeroBus development, use Java 25 explicitly:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home \
  mvn -pl debezium-server-zerobus test -DskipITs -Dquick -DskipTests=false -Dformat.skip=true
```

To verify generated Debezium Server sink metadata:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home \
  mvn -pl debezium-server-zerobus prepare-package -DskipITs -Dquick -DskipTests=true -Dformat.skip=true
```

## Kafka-Compatible Recipe Boundary

ZeroBus also supports a Kafka-compatible API path that can be useful for Kafka Connect deployments:

```properties
producer.override.bootstrap.servers=<zerobus-kafka-compatible-endpoint>
producer.override.sasl.login.callback.handler.class=<zerobus-oauth-handler>
transforms=<RegexRouter mapping>
tombstones.on.delete=false
```

That recipe removes Kafka broker from the data path, but still runs Kafka Connect and still uses
Kafka producer-compatible APIs. It is separate from this native Debezium Server sink.
