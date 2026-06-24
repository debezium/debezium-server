# Debezium Server ZeroBus Sink Contribution Packet

## Summary

This contribution adds a native Debezium Server sink named `zerobus`.

The native data path is:

```text
Debezium Server source connector
  -> debezium.sink.type=zerobus
  -> Databricks ZeroBus Java SDK JSON, protobuf, or Arrow stream
  -> Unity Catalog target table
```

This is not a Kafka Connect sink and does not use a Kafka broker, Kafka producer
client, or Kafka Connect SMTs in the sink implementation.

## Scope

Included:

- New `debezium-server-zerobus` module.
- CDI sink named `zerobus`.
- Config model under `debezium.sink.zerobus.*`.
- Databricks ZeroBus Java SDK client boundary.
- JSON, protobuf, and Arrow envelope serialization.
- Unity Catalog table routing.
- Write-before-offset-commit batch behavior.
- Source-order-preserving writes across interleaved target tables.
- Retry handling and partial-ack failure handling.
- Deterministic source-position idempotency key mode for retry stability and
  LSN/transaction ordering.
- Mocked unit tests.
- Debezium Server distribution and BOM wiring.
- Generated Debezium Server sink metadata.
- User docs and a separate Kafka-compatible recipe boundary.

Out of scope:

- Kafka Connect runtime support for this native sink.
- Kafka broker or Kafka producer API usage.
- Kafka Connect SMT-based table routing.
- Automatic Unity Catalog table creation.
- Full schema evolution management.
- Table-specific typed protobuf row generation.
- Source-table-shaped Arrow row generation.
- Live ZeroBus integration tests in default CI.

## Configuration

Minimal sink configuration:

```properties
debezium.sink.type=zerobus
debezium.sink.zerobus.endpoint=https://zerobus.example
debezium.sink.zerobus.workspace.url=https://dbc-a1b2c3d4-e5f6.cloud.databricks.com
debezium.sink.zerobus.authentication.type=oauth2
debezium.sink.zerobus.authentication.oauth2.client-id=${ZEROBUS_CLIENT_ID}
debezium.sink.zerobus.authentication.oauth2.client-secret=${ZEROBUS_CLIENT_SECRET}
debezium.sink.zerobus.record.format=json
debezium.sink.zerobus.idempotency.mode=source
debezium.sink.zerobus.tombstone.handling.mode=event
debezium.sink.zerobus.table.mapping.mode=source
debezium.sink.zerobus.table.mapping.default.catalog=main
debezium.sink.zerobus.table.mapping.default.schema=bronze
```

Supported table mapping modes:

- `source`: maps the last segment of the Debezium destination to
  `<default.catalog>.<default.schema>.<table>`.
- `explicit`: maps exact Debezium destinations using
  `table.mapping.overrides`.
- `regex`: applies a native regex and replacement to the Debezium destination.

Supported tombstone handling modes:

- `event`: write a ZeroBus row with `operation=tombstone` and `value=null`.
- `drop`: skip null-value tombstones and still allow Debezium offsets to
  advance after the rest of the batch succeeds.

Real delete events remain separate from tombstones. When the Debezium envelope
contains `op=d`, the sink emits `operation=delete`.

Supported idempotency modes:

- `source`: write a deterministic `idempotency_key` from Debezium destination,
  partition, key, and the underlying `SourceRecord` source partition/offset
  when available. This carries connector offsets such as LSN and transaction
  metadata into the ZeroBus row. Header fingerprints are used only as a fallback
  for non-embedded events.
- `none`: omit the key when the deployment has another dedupe contract.

## Record Shape

The first contribution implements `record.format=json`, `record.format=protobuf`, and
`record.format=arrow`.

JSON rows written to ZeroBus contain:

```json
{
  "target_table": "main.bronze.customers",
  "destination": "server.inventory.customers",
  "partition": 0,
  "operation": "update",
  "idempotency_key": "server.inventory.customers|partition=0|key=1|source_position=offset.event_serial_no=3&offset.lsn=123456789&offset.txId=42&partition.server=inventory",
  "key": {},
  "value": {},
  "source_position": {
    "partition.server": "inventory",
    "offset.lsn": "123456789",
    "offset.txId": "42",
    "offset.event_serial_no": "3"
  },
  "headers": {}
}
```

When Debezium key or value payloads are already JSON strings, the serializer
embeds them as JSON objects instead of double-quoted text. Non-JSON strings are
preserved as text values.

Protobuf rows use the SDK's `ZerobusProtoStream` with a stable
`DebeziumZeroBusRecord` descriptor:

```protobuf
message DebeziumZeroBusRecord {
  string target_table = 1;
  string destination = 2;
  int32 partition = 3;
  string operation = 4;
  string idempotency_key = 5;
  string key = 6;
  string value = 7;
  string source_position = 8;
  string headers = 9;
}
```

In protobuf mode, `key`, `value`, `source_position`, and `headers` are canonical
JSON strings inside the envelope. Table-specific typed protobuf rows are parked
as a schema-evolution follow-up.

Arrow rows use the SDK's `ZerobusArrowStream` with the same stable envelope:

```text
target_table: Utf8
destination: Utf8
partition: Int(32, signed)
operation: Utf8
idempotency_key: Utf8
key: Utf8
value: Utf8
source_position: Utf8
headers: Utf8
```

In Arrow mode, `key`, `value`, `source_position`, and `headers` are canonical
JSON strings in nullable UTF-8 columns. Source-table-shaped Arrow rows are parked
as a schema-evolution follow-up and are separate from the earlier embedded
Arrow exporter project.

## Offset And Ack Semantics

The sink preserves Debezium's at-least-once boundary:

1. Debezium Server passes a batch of `ChangeEvent<Object, Object>` records.
2. The sink maps each event to a `ZeroBusRecord`, including the underlying
   `SourceRecord` source partition/offset when Debezium Server exposes an
   `EmbeddedEngineChangeEvent`.
3. Records are mapped into target Unity Catalog table rows.
4. The writer preserves Debezium input order across interleaved target tables
   and batches consecutive records for the same target table.
5. The SDK-backed client writes each chunk with `ingestRecordsOffset(...)` for
   JSON/protobuf streams or `ingestBatch(...)` for Arrow streams.
6. If the SDK returns an offset, the client waits with `waitForOffset(...)`.
7. Only after all chunks are acknowledged does the consumer call
   `committer.markProcessed(...)` for the processed records and
   `committer.markBatchFinished()`.

If a write fails, the sink does not mark records processed. If ZeroBus partially
acknowledges a chunk, the sink fails the batch before advancing offsets.

The first contribution is at-least-once. Exactly-once semantics depend on a live
service/table dedupe contract and are not claimed by this module.

## Source Files

| File | Purpose |
|---|---|
| `ZeroBusChangeConsumer.java` | Debezium Server sink entry point and committer boundary. |
| `ZeroBusSinkConfig.java` | Config fields, defaults, parsing, and validation. |
| `DatabricksZeroBusClient.java` | SDK-backed client implementation. |
| `ZeroBusSdkAdapter.java` | Thin SDK adapter boundary around JSON/protobuf/Arrow streams. |
| `ZeroBusClient.java` | Mockable client interface. |
| `ZeroBusBatchWriter.java` | Grouping, batching, retry, partial-ack handling. |
| `ZeroBusRecordMapper.java` | `ChangeEvent` to native record mapping, source-position capture, and idempotency keys. |
| `ZeroBusTableRouter.java` | Source, explicit, and regex Unity Catalog table routing. |
| `ZeroBusJsonSerializer.java` | JSON row serialization. |
| `ZeroBusProtobufSerializer.java` | Protobuf descriptor and envelope serialization. |
| `ZeroBusArrowSerializer.java` | Arrow schema and batch serialization. |
| `ZeroBusRecord.java` | Native sink record model. |
| `ZeroBusWriteResult.java` | Ack result model. |
| `ZeroBusRetriableException.java` | Retryable write failure signal. |
| `META-INF/services/io.debezium.metadata.ComponentMetadataProvider` | Debezium metadata generator discovery. |

## Tests

Mocked tests cover:

- Required config validation.
- JSON, protobuf, and Arrow record format validation.
- Unsupported record format rejection.
- Unsupported idempotency mode rejection.
- Source, explicit, and regex table routing.
- Invalid Unity Catalog identifiers.
- Delete event mapping from Debezium envelope `op=d`.
- Dropped tombstone handling.
- Retry with a stable idempotency key.
- SourceRecord source offset/partition capture for LSN-aware idempotency.
- Source-order preservation across interleaved target tables.
- Same-table batching for consecutive records.
- Partial acknowledgement failure.
- No offset commit on write failure.
- Marking records processed only after durable ack.
- JSON payload embedding and non-JSON string preservation.
- Protobuf descriptor generation and dynamic protobuf payload parsing.
- JSON/protobuf/Arrow SDK stream selection.
- Arrow schema generation and batch payload serialization.
- Arrow in-flight batch configuration validation.
- Disabled idempotency serialization.

The focused module command is:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home \
  mvn -pl debezium-server-zerobus test -DskipITs -Dquick -DskipTests=false -Dformat.skip=true
```

## Packaging And Metadata

The module is wired into:

- root `pom.xml`
- `debezium-server-bom/pom.xml`
- `debezium-server-dist/pom.xml`
- custom distribution profile `sink-zerobus`

Generated sink metadata is verified with:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home \
  mvn -pl debezium-server-zerobus prepare-package -DskipITs -Dquick -DskipTests=true -Dformat.skip=true
```

Expected generated descriptor:

```text
target/classes/META-INF/descriptors/server-sink/io.debezium.server.zerobus.ZeroBusChangeConsumer.json
```

The descriptor must include endpoint, workspace URL, OAuth client fields, record
format, table mapping fields, JSON/protobuf in-flight records, Arrow in-flight
batches, idempotency mode, and tombstone handling mode.

## Kafka-Compatible Recipe Boundary

ZeroBus also supports a Kafka-compatible API path for deployments that still run
Kafka Connect:

```properties
producer.override.bootstrap.servers=<zerobus-kafka-compatible-endpoint>
producer.override.sasl.login.callback.handler.class=<zerobus-oauth-handler>
transforms=<RegexRouter mapping>
tombstones.on.delete=false
```

That recipe removes Kafka broker from the data path, but it still runs Kafka
Connect and still uses Kafka producer-compatible APIs. It is documentation-only
for this contribution and is not the native Debezium Server sink.

## Follow-Up Decisions

These are intentionally parked outside the first contribution:

- Live ZeroBus integration-test strategy.
- Whether upstream CI should have a disabled-by-default integration profile.
- Table pre-creation and schema evolution policy.
- Downstream Delta merge/delete/upsert guidance.
- Table-specific typed protobuf row generation.
- Source-table-shaped Arrow row generation.
- Exact service-side dedupe guarantees beyond the emitted `idempotency_key`.
