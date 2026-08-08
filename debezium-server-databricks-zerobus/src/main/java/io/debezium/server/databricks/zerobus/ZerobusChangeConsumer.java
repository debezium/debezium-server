/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.api.DebeziumServerConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics;

/**
 * Debezium Server sink that writes change events directly into Databricks Zerobus Ingest using the
 * Zerobus Java SDK (gRPC transport, GA). Typed mode normalizes each event to the target table's
 * columns and writes JSON. Envelope mode preserves key, value, source position, headers, operation,
 * and deterministic source identity, encoded as JSON or Protobuf. Both modes use a per-table
 * {@link ZerobusStreamHandle}.
 * <p>
 * Zerobus binds one stream to one managed Delta table. This consumer therefore keeps a stream per
 * resolved table name. The target table for a record is the mapped destination (topic) name, unless
 * a fully-qualified default table is configured via {@code debezium.sink.zerobus.table}.
 * <p>
 * Delivery is at-least-once: each batch is flushed to durability before the records are committed,
 * so a crash after flush but before commit may re-send records. Deduplicate downstream using the
 * Debezium source LSN/offset carried in the payload.
 */
@Named("zerobus")
@Dependent
public class ZerobusChangeConsumer extends BaseChangeConsumer
        implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerobusChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.zerobus.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private ZerobusChangeConsumerConfig config;
    private ZerobusSdk sdk;
    private final ZerobusSinkMetrics metrics = new ZerobusSinkMetrics("grpc");
    private long batchesSinceLog = 0;

    // One stream per target table (Zerobus: 1 stream = 1 table). Access-ordered so that iteration
    // yields the least recently used table first, which is what bounds the number of open streams.
    private final Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
    private final Map<String, ZerobusStreamHandle<byte[]>> protobufStreams = new LinkedHashMap<>(16, 0.75f, true);
    private ZerobusEnvelopeMapper envelopeMapper;
    private ZerobusEventFilter envelopeFilter;
    private ZerobusJsonEnvelopeSerializer jsonEnvelopeSerializer;
    private ZerobusProtobufEnvelopeSerializer protobufEnvelopeSerializer;

    /**
     * Lets a deployment supply its own {@link ZerobusSdk}, for example to point at a different
     * endpoint or to wrap the client, following the injection point the other Debezium Server sinks
     * expose. When no such bean is provided, the SDK is built from the connector configuration.
     */
    @Inject
    @CustomConsumerBuilder
    Instance<ZerobusSdk> customZerobusSdk;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new ZerobusChangeConsumerConfig(configuration);

        configureEnvelopePath();

        if (customZerobusSdk.isResolvable()) {
            this.sdk = customZerobusSdk.get();
            LOGGER.info("Obtained custom configured ZerobusSdk '{}'", sdk);
        }
        else {
            this.sdk = new ZerobusSdk(config.getEndpoint(), config.getWorkspaceUrl());
        }
        metrics.register();
        metrics.setConnected(true);
        LOGGER.info("Zerobus gRPC sink connected: endpoint={}, workspaceUrl={}", config.getEndpoint(), config.getWorkspaceUrl());
    }

    /**
     * Asks the engine for tombstones only when the envelope payload mode is configured to write them.
     * <p>
     * Without this the engine defaults to withholding tombstones, so
     * {@code tombstone.handling.mode=event} could never produce a {@code TOMBSTONE} envelope. The typed
     * and REST paths are unaffected either way: they discard a null payload in {@code isJsonObject}.
     * <p>
     * The configuration is read here rather than from {@link #config}, because the engine queries this
     * capability while deciding what to capture, which is not ordered against {@code @PostConstruct}.
     */
    @Override
    public Optional<Boolean> tombstoneSupport() {
        final ZerobusChangeConsumerConfig capabilityConfig = new ZerobusChangeConsumerConfig(
                io.debezium.config.Configuration.from(getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX)));
        return Optional.of(capabilityConfig.getPayloadMode() == ZerobusChangeConsumerConfig.PayloadMode.ENVELOPE
                && ZerobusChangeConsumerConfig.TOMBSTONE_EVENT.equals(capabilityConfig.getTombstoneHandlingMode()));
    }

    @PreDestroy
    @Override
    public void close() {
        closeStreams(streams);
        closeStreams(protobufStreams);
        if (sdk != null) {
            sdk.close();
        }
        metrics.setConnected(false);
        metrics.unregister();
        LOGGER.info("Zerobus gRPC sink closed");
    }

    private <P> void closeStreams(Map<String, ZerobusStreamHandle<P>> openStreams) {
        for (Map.Entry<String, ZerobusStreamHandle<P>> entry : openStreams.entrySet()) {
            try {
                entry.getValue().close();
                metrics.streamClosed();
            }
            catch (Exception e) {
                LOGGER.warn("Could not close Zerobus stream for table '{}'", entry.getKey(), e);
            }
        }
        openStreams.clear();
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {
        if (config.getPayloadMode() == ZerobusChangeConsumerConfig.PayloadMode.ENVELOPE) {
            handleEnvelope(events);
            return;
        }
        handleTyped(events);
    }

    private void handleTyped(CapturingEvents<BatchEvent> events) throws InterruptedException {

        // Track only the streams that actually received a record in this batch, so we flush those
        // rather than every open stream: with N tables but only a few touched per batch, flushing
        // all N would add avoidable round-trips.
        final Set<String> touchedTables = new HashSet<>();

        // Metadata of the records handed to the SDK, counted only once the flush below makes them
        // durable. Counting at ingestion time would over-report: a record enqueued on a stream whose
        // flush then fails never became durable, yet would already have incremented the counters.
        final List<String> ingestedOperations = new ArrayList<>(events.records().size());
        long lastSourceTsMs = -1L;

        for (BatchEvent record : events.records()) {
            String json = toZerobusJson(record, this::getString);
            String table = resolveTable(record.destination());
            if (isJsonObject(json) && table != null) {
                LOGGER.debug("Ingesting into {}: {}", table, json);
                try {
                    stream(table).ingest(json);
                    touchedTables.add(table);
                    ingestedOperations.add(operationOf(record));
                    final long sourceTsMs = sourceTsMsOf(record);
                    if (sourceTsMs >= 0) {
                        lastSourceTsMs = sourceTsMs;
                    }
                }
                catch (Exception e) {
                    metrics.recordError();
                    throw new DebeziumException("Failed to ingest record into Zerobus table '" + table + "'", e);
                }
            }
            else {
                // Skip records that are not ingestable: tombstones / null payloads, and events whose
                // destination is not a fully-qualified table (e.g. MySQL schema-history / DDL events,
                // which the binlog connector emits on the topic-prefix "topic").
                metrics.recordSkipped();
                LOGGER.trace("Skipping record for destination '{}' (table={}): {}", record.destination(), table, json);
            }
        }

        // Flush only the streams touched in this batch to durability, then acknowledge the offsets.
        // The commit must happen after the flush: committing first would advance the source offset
        // while the ingested records are still buffered, so a crash between commit and flush would
        // drop them (breaking the at-least-once guarantee).
        final long flushStartNanos = System.nanoTime();
        for (String table : touchedTables) {
            try {
                streams.get(table).flush();
            }
            catch (Exception e) {
                metrics.recordError();
                throw new DebeziumException("Failed to flush Zerobus stream for table '" + table + "'", e);
            }
        }
        metrics.flushed((System.nanoTime() - flushStartNanos) / 1_000_000L);

        // Past the durability barrier, so the batch can now be counted as ingested. The source
        // timestamp of the last durable record drives the freshness gauge.
        for (String operation : ingestedOperations) {
            metrics.recordIngested(operation, -1L);
        }
        if (lastSourceTsMs >= 0) {
            metrics.recordSourceTsMs(lastSourceTsMs);
        }

        for (BatchEvent record : events.records()) {
            record.commit();
        }
        evictStreamsAboveLimit();
        maybeLogMetrics();
    }

    private void handleEnvelope(CapturingEvents<BatchEvent> events) {
        configureEnvelopePath();
        List<ZerobusEnvelope> contiguousTableRecords = new ArrayList<>();
        String currentTable = null;

        for (BatchEvent record : events.records()) {
            if (record.value() == null
                    && ZerobusChangeConsumerConfig.TOMBSTONE_DROP.equals(config.getTombstoneHandlingMode())) {
                metrics.recordSkipped();
                continue;
            }

            ZerobusEventFilter.Decision decision;
            try {
                decision = envelopeFilter.evaluate(record);
            }
            catch (DebeziumException e) {
                metrics.recordError();
                throw e;
            }
            if (!decision.accepted()) {
                metrics.recordSkipped();
                LOGGER.debug("Skipping Zerobus envelope for destination '{}' because filter '{}' did not match",
                        record.destination(), decision.reason());
                continue;
            }

            String table = resolveTable(record.destination());
            if (table == null) {
                metrics.recordSkipped();
                LOGGER.trace("Skipping Zerobus envelope for non-table destination '{}'", record.destination());
                continue;
            }

            if (currentTable != null && !currentTable.equals(table)) {
                writeEnvelopeGroup(currentTable, contiguousTableRecords);
                contiguousTableRecords.clear();
            }
            currentTable = table;
            try {
                contiguousTableRecords.add(envelopeMapper.map(record, table));
            }
            catch (RuntimeException e) {
                metrics.recordError();
                throw new DebeziumException("Failed to map Zerobus envelope for destination '"
                        + record.destination() + "'", e);
            }
        }

        if (!contiguousTableRecords.isEmpty()) {
            writeEnvelopeGroup(currentTable, contiguousTableRecords);
        }

        for (BatchEvent record : events.records()) {
            record.commit();
        }
        evictEnvelopeStreamsAboveLimit();
        maybeLogMetrics();
    }

    private void configureEnvelopePath() {
        if (config == null || config.getPayloadMode() != ZerobusChangeConsumerConfig.PayloadMode.ENVELOPE
                || envelopeMapper != null) {
            return;
        }
        envelopeMapper = new ZerobusEnvelopeMapper(config);
        envelopeFilter = new ZerobusEventFilter(config, envelopeMapper);
        jsonEnvelopeSerializer = new ZerobusJsonEnvelopeSerializer(config);
        protobufEnvelopeSerializer = new ZerobusProtobufEnvelopeSerializer();
    }

    private void writeEnvelopeGroup(String table, List<ZerobusEnvelope> records) {
        try {
            long started = System.nanoTime();
            if (config.getRecordFormat() == ZerobusChangeConsumerConfig.RecordFormat.PROTOBUF) {
                writeProtobufEnvelopeGroup(table, records);
            }
            else {
                writeJsonEnvelopeGroup(table, records);
            }
            metrics.flushed((System.nanoTime() - started) / 1_000_000L);
            for (ZerobusEnvelope record : records) {
                metrics.recordIngested(metricOperation(record.operation()),
                        record.sourceTimestampMillis() == null ? -1L : record.sourceTimestampMillis());
            }
        }
        catch (DebeziumException e) {
            metrics.recordError();
            throw e;
        }
        catch (Exception e) {
            metrics.recordError();
            throw new DebeziumException("Failed to durably ingest Zerobus envelope group for table '" + table + "'", e);
        }
    }

    private void writeJsonEnvelopeGroup(String table, List<ZerobusEnvelope> records) throws Exception {
        List<String> payloads = new ArrayList<>(records.size());
        for (ZerobusEnvelope record : records) {
            String payload = jsonEnvelopeSerializer.serialize(record);
            validatePayloadSize(record, payload.getBytes(StandardCharsets.UTF_8).length);
            payloads.add(payload);
        }

        ZerobusStreamHandle<String> stream = stream(table);
        long lastOffset = -1L;
        for (String payload : payloads) {
            lastOffset = stream.ingest(payload);
        }
        stream.waitForOffset(lastOffset);
    }

    private void writeProtobufEnvelopeGroup(String table, List<ZerobusEnvelope> records) throws Exception {
        List<byte[]> payloads = new ArrayList<>(records.size());
        for (ZerobusEnvelope record : records) {
            byte[] payload = protobufEnvelopeSerializer.serialize(record);
            validatePayloadSize(record, payload.length);
            payloads.add(payload);
        }

        ZerobusStreamHandle<byte[]> stream = protobufStream(table);
        long lastOffset = -1L;
        for (byte[] payload : payloads) {
            lastOffset = stream.ingest(payload);
        }
        stream.waitForOffset(lastOffset);
    }

    private void validatePayloadSize(ZerobusEnvelope record, int payloadBytes) {
        if (payloadBytes > config.getMaxRecordBytes()) {
            throw new DebeziumException("Zerobus " + config.getRecordFormat().getValue() + " envelope for destination "
                    + record.destination() + " mapped to table " + record.targetTable() + " is " + payloadBytes
                    + " bytes, exceeding max.record.bytes=" + config.getMaxRecordBytes()
                    + "; source_position=" + record.sourcePosition());
        }
    }

    private static String metricOperation(ZerobusOperation operation) {
        return switch (operation) {
            case CREATE -> "c";
            case READ -> "r";
            case UPDATE -> "u";
            case DELETE -> "d";
            case CHANGE, TOMBSTONE -> null;
        };
    }

    private void maybeLogMetrics() {
        final int interval = config.getMetricsLogInterval();
        if (interval > 0 && ++batchesSinceLog >= interval) {
            batchesSinceLog = 0;
            metrics.logMetricsSummary();
        }
    }

    /**
     * Resolves the target table for a record. If a default fully-qualified table is configured, it
     * always wins; otherwise the mapped destination (topic) name is used and must already be a
     * {@code catalog.schema.table} identifier. Returns {@code null} when the destination is not a
     * fully-qualified table (e.g. schema-history / DDL events emitted on the topic prefix), so the
     * caller can skip it rather than opening a bogus stream.
     */
    private String resolveTable(String destination) {
        String configured = config.getTable();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        String mapped = streamNameMapper.map(destination);
        return isQualifiedTable(mapped) ? mapped : null;
    }

    /** True when {@code name} is a fully-qualified {@code catalog.schema.table} identifier. */
    static boolean isQualifiedTable(String name) {
        if (name == null) {
            return false;
        }
        String[] parts = name.split("\\.");
        if (parts.length != 3) {
            return false;
        }
        for (String p : parts) {
            if (p.isBlank()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Produces the JSON string to ingest for a change event, shared by the gRPC and REST routes.
     * <p>
     * When the typed {@link org.apache.kafka.connect.data.Struct} is available (via
     * {@link BatchEvent#record()}), the {@link ZerobusTypeSystem} normalizes each value to the shape
     * Zerobus/Delta expects (decimal→string, bytes→base64, temporals→epoch, JSON→VARIANT string, …),
     * which avoids the "invalid type" (4044) class of errors without requiring source-side
     * {@code *.handling.mode} tuning. If the record carries no schema (schemaless / already-JSON),
     * it falls back to the verbatim JSON produced by {@code debezium.format.value=json}.
     */
    static String toZerobusJson(BatchEvent record, java.util.function.Function<Object, String> verbatim) {
        org.apache.kafka.connect.source.SourceRecord source = record.record();
        if (source != null && source.valueSchema() != null && source.value() != null) {
            return ZerobusTypeSystem.normalizeToJson(source.valueSchema(), source.value());
        }
        // fallback: no typed schema available — forward the JSON Debezium already serialized
        return record.value() == null ? null : verbatim.apply(record.value());
    }

    /**
     * Extracts the Debezium operation ({@code c}/{@code u}/{@code d}/{@code r}) from a change event, or
     * {@code null} when unavailable. Shared by the gRPC and REST routes to split ingestion metrics by
     * operation. Handles both shapes the sink sees in practice:
     * <ul>
     * <li>the full Debezium envelope, which carries {@code op} at the top level;</li>
     * <li>an unwrapped record (the recommended {@code ExtractNewRecordState} setup with
     * {@code add.fields=op,...}), which exposes it as {@code __op} (default {@code __} prefix).</li>
     * </ul>
     */
    static String operationOf(BatchEvent record) {
        org.apache.kafka.connect.data.Struct struct = valueStruct(record);
        if (struct == null) {
            return null;
        }
        if (struct.schema().field("op") != null) {
            return struct.getString("op");
        }
        if (struct.schema().field("__op") != null) {
            return struct.getString("__op");
        }
        return null;
    }

    /**
     * Extracts {@code source.ts_ms} (the source database change timestamp) from a change event, or
     * {@code -1} when unavailable. Used to compute the sink's freshness / lag behind the source.
     * Handles both the full envelope (nested {@code source.ts_ms}) and an unwrapped record with
     * {@code add.fields=source.ts_ms} (exposed as the top-level {@code __source_ts_ms}).
     */
    static long sourceTsMsOf(BatchEvent record) {
        org.apache.kafka.connect.data.Struct struct = valueStruct(record);
        if (struct == null) {
            return -1L;
        }
        // Envelope: nested source.ts_ms
        if (struct.schema().field("source") != null && struct.get("source") instanceof org.apache.kafka.connect.data.Struct) {
            org.apache.kafka.connect.data.Struct sourceStruct = (org.apache.kafka.connect.data.Struct) struct.get("source");
            long ts = longField(sourceStruct, "ts_ms");
            if (ts >= 0) {
                return ts;
            }
        }
        // Unwrapped: flattened __source_ts_ms
        return longField(struct, "__source_ts_ms");
    }

    private static org.apache.kafka.connect.data.Struct valueStruct(BatchEvent record) {
        Object value = record.record() == null ? null : record.record().value();
        return value instanceof org.apache.kafka.connect.data.Struct ? (org.apache.kafka.connect.data.Struct) value : null;
    }

    private static long longField(org.apache.kafka.connect.data.Struct struct, String field) {
        if (struct.schema().field(field) != null) {
            Object v = struct.get(field);
            if (v instanceof Number) {
                return ((Number) v).longValue();
            }
        }
        return -1L;
    }

    /** Zerobus only accepts JSON objects; a null or "null"/non-object payload (a tombstone) must be skipped. */
    static boolean isJsonObject(String json) {
        if (json == null) {
            return false;
        }
        String trimmed = json.strip();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }

    private ZerobusStreamHandle<String> stream(String table) {
        return streams.computeIfAbsent(table, this::createStream);
    }

    /**
     * Closes the least recently used streams until at most {@code max.open.streams} remain open.
     * <p>
     * Zerobus binds one stream to one table, so a source with many tables would otherwise accumulate
     * an unbounded number of open streams, each holding a connection. A stream is flushed before it is
     * closed, so evicting one never drops buffered records; it is reopened on demand if the table is
     * written to again. Eviction runs after the batch has been flushed and committed, so it never
     * closes a stream this batch still depends on.
     */
    private void evictStreamsAboveLimit() {
        evictStreamsAboveLimit(streams);
    }

    private void evictEnvelopeStreamsAboveLimit() {
        if (config.getRecordFormat() == ZerobusChangeConsumerConfig.RecordFormat.PROTOBUF) {
            evictStreamsAboveLimit(protobufStreams);
        }
        else {
            evictStreamsAboveLimit(streams);
        }
    }

    private <P> void evictStreamsAboveLimit(Map<String, ZerobusStreamHandle<P>> openStreams) {
        final int limit = config.getMaxOpenStreams();
        if (limit <= 0) {
            return;
        }
        final Iterator<Map.Entry<String, ZerobusStreamHandle<P>>> iterator = openStreams.entrySet().iterator();
        while (openStreams.size() > limit && iterator.hasNext()) {
            final Map.Entry<String, ZerobusStreamHandle<P>> eldest = iterator.next();
            try {
                eldest.getValue().flush();
                eldest.getValue().close();
                metrics.streamClosed();
                LOGGER.debug("Closed least recently used Zerobus stream for table '{}' to stay within {} open streams",
                        eldest.getKey(), limit);
            }
            catch (Exception e) {
                // An eviction failure must not fail the batch: the records were already flushed and
                // committed, and the stream is dropped from the map either way.
                LOGGER.warn("Could not cleanly close Zerobus stream for table '{}' during eviction", eldest.getKey(), e);
            }
            iterator.remove();
        }
    }

    private ZerobusStreamHandle<String> createStream(String table) {
        try {
            LOGGER.info("Opening Zerobus JSON stream for table '{}'", table);
            StreamConfigurationOptions options = recoveryOptions(StreamConfigurationOptions.builder()
                    .setMaxInflightRecords(config.getMaxInflightRecords()))
                    .build();
            ZerobusJsonStream openedStream = sdk.createJsonStream(table, config.getClientId(), config.getClientSecret(), options).join();
            metrics.streamOpened();
            return new ZerobusJsonStreamHandle(openedStream);
        }
        catch (Exception e) {
            throw new DebeziumException("Could not open Zerobus stream for table '" + table + "'", e);
        }
    }

    private ZerobusStreamHandle<byte[]> protobufStream(String table) {
        return protobufStreams.computeIfAbsent(table, this::createProtobufStream);
    }

    private ZerobusStreamHandle<byte[]> createProtobufStream(String table) {
        try {
            LOGGER.info("Opening Zerobus Protobuf stream for table '{}'", table);
            StreamConfigurationOptions options = recoveryOptions(StreamConfigurationOptions.builder()
                    .setMaxInflightRecords(config.getMaxInflightRecords()))
                    .build();
            ZerobusProtoStream openedStream = sdk.createProtoStream(table, protobufEnvelopeSerializer.descriptorProto(),
                    config.getClientId(), config.getClientSecret(), options).join();
            metrics.streamOpened();
            return new ZerobusProtoStreamHandle(openedStream);
        }
        catch (Exception e) {
            throw new DebeziumException("Could not open Zerobus Protobuf stream for table '" + table + "'", e);
        }
    }

    /**
     * Applies the stream recovery options that are configured, leaving the others at whatever the SDK
     * defaults to.
     * <p>
     * With recovery enabled the SDK re-sends the records a failed stream has not acknowledged, which is
     * safe precisely because it knows which those are; the sink therefore does not retry the batch
     * itself and cannot duplicate records that already landed. An error that recovery cannot resolve
     * still propagates, so the offset is not committed past it.
     */
    static StreamConfigurationOptions.StreamConfigurationOptionsBuilder recoveryOptions(
                                                                                        StreamConfigurationOptions.StreamConfigurationOptionsBuilder builder,
                                                                                        ZerobusChangeConsumerConfig config) {
        if (config.getRecovery() != null) {
            builder.setRecovery(config.getRecovery());
        }
        if (config.getRecoveryRetries() != null) {
            builder.setRecoveryRetries(config.getRecoveryRetries());
        }
        if (config.getRecoveryBackoffMs() != null) {
            builder.setRecoveryBackoffMs(config.getRecoveryBackoffMs());
        }
        if (config.getRecoveryTimeoutMs() != null) {
            builder.setRecoveryTimeoutMs(config.getRecoveryTimeoutMs());
        }
        if (config.getFlushTimeoutMs() != null) {
            builder.setFlushTimeoutMs(config.getFlushTimeoutMs());
        }
        return builder;
    }

    private StreamConfigurationOptions.StreamConfigurationOptionsBuilder recoveryOptions(
                                                                                         StreamConfigurationOptions.StreamConfigurationOptionsBuilder builder) {
        return recoveryOptions(builder, config);
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                ZerobusChangeConsumerConfig.ENDPOINT,
                ZerobusChangeConsumerConfig.WORKSPACE_URL,
                ZerobusChangeConsumerConfig.CLIENT_ID,
                ZerobusChangeConsumerConfig.CLIENT_SECRET,
                ZerobusChangeConsumerConfig.TABLE,
                ZerobusChangeConsumerConfig.PAYLOAD_MODE,
                ZerobusChangeConsumerConfig.RECORD_FORMAT,
                ZerobusChangeConsumerConfig.MAX_RECORD_BYTES,
                ZerobusChangeConsumerConfig.JSON_FLEXIBLE_FIELDS_ENCODING,
                ZerobusChangeConsumerConfig.IDEMPOTENCY_MODE,
                ZerobusChangeConsumerConfig.TOMBSTONE_HANDLING_MODE,
                ZerobusChangeConsumerConfig.FILTER_DESTINATION_REGEX,
                ZerobusChangeConsumerConfig.FILTER_OPERATIONS,
                ZerobusChangeConsumerConfig.FILTER_HEADER_NAME,
                ZerobusChangeConsumerConfig.FILTER_HEADER_VALUE_REGEX,
                ZerobusChangeConsumerConfig.FILTER_VALUE_JSON_POINTER,
                ZerobusChangeConsumerConfig.FILTER_VALUE_REGEX,
                ZerobusChangeConsumerConfig.FILTER_MALFORMED_MODE,
                ZerobusChangeConsumerConfig.MAX_INFLIGHT_RECORDS,
                ZerobusChangeConsumerConfig.MAX_OPEN_STREAMS,
                ZerobusChangeConsumerConfig.RECOVERY,
                ZerobusChangeConsumerConfig.RECOVERY_RETRIES,
                ZerobusChangeConsumerConfig.RECOVERY_BACKOFF_MS,
                ZerobusChangeConsumerConfig.RECOVERY_TIMEOUT_MS,
                ZerobusChangeConsumerConfig.FLUSH_TIMEOUT_MS,
                ZerobusChangeConsumerConfig.METRICS_LOG_INTERVAL);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
