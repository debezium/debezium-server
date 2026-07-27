/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerConsumer;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Debezium Server sink that writes change events directly into Databricks Zerobus Ingest using the
 * Zerobus Java SDK (gRPC transport, GA). Change events are serialized as JSON by Debezium
 * ({@code debezium.format.value=json}) and forwarded verbatim through a per-table
 * {@link ZerobusJsonStream}.
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

    // One stream per target table (Zerobus: 1 stream = 1 table).
    private final Map<String, ZerobusJsonStream> streams = new HashMap<>();

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new ZerobusChangeConsumerConfig(configuration);

        this.sdk = new ZerobusSdk(config.getEndpoint(), config.getWorkspaceUrl());
        LOGGER.info("Zerobus gRPC sink connected: endpoint={}, workspaceUrl={}", config.getEndpoint(), config.getWorkspaceUrl());
    }

    @PreDestroy
    @Override
    public void close() {
        for (Map.Entry<String, ZerobusJsonStream> entry : streams.entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                LOGGER.warn("Could not close Zerobus stream for table '{}'", entry.getKey(), e);
            }
        }
        streams.clear();
        if (sdk != null) {
            sdk.close();
        }
        LOGGER.info("Zerobus gRPC sink closed");
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {

        for (BatchEvent record : events.records()) {
            String json = toZerobusJson(record, this::getString);
            String table = resolveTable(record.destination());
            if (isJsonObject(json) && table != null) {
                LOGGER.debug("Ingesting into {}: {}", table, json);
                try {
                    stream(table).ingestRecordOffset(json);
                }
                catch (Exception e) {
                    throw new DebeziumException("Failed to ingest record into Zerobus table '" + table + "'", e);
                }
            }
            else {
                // Skip records that are not ingestable: tombstones / null payloads, and events whose
                // destination is not a fully-qualified table (e.g. MySQL schema-history / DDL events,
                // which the binlog connector emits on the topic-prefix "topic").
                LOGGER.trace("Skipping record for destination '{}' (table={}): {}", record.destination(), table, json);
            }
            record.commit();
        }

        // Flush every touched stream to durability before acknowledging the batch (at-least-once).
        for (Map.Entry<String, ZerobusJsonStream> entry : streams.entrySet()) {
            try {
                entry.getValue().flush();
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to flush Zerobus stream for table '" + entry.getKey() + "'", e);
            }
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

    /** Zerobus only accepts JSON objects; a null or "null"/non-object payload (a tombstone) must be skipped. */
    static boolean isJsonObject(String json) {
        if (json == null) {
            return false;
        }
        String trimmed = json.strip();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }

    private ZerobusJsonStream stream(String table) {
        return streams.computeIfAbsent(table, this::createStream);
    }

    private ZerobusJsonStream createStream(String table) {
        try {
            LOGGER.info("Opening Zerobus JSON stream for table '{}'", table);
            StreamConfigurationOptions options = StreamConfigurationOptions.builder()
                    .setMaxInflightRecords(config.getMaxInflightRecords())
                    .build();
            return sdk.createJsonStream(table, config.getClientId(), config.getClientSecret(), options).join();
        }
        catch (Exception e) {
            throw new DebeziumException("Could not open Zerobus stream for table '" + table + "'", e);
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                ZerobusChangeConsumerConfig.ENDPOINT,
                ZerobusChangeConsumerConfig.WORKSPACE_URL,
                ZerobusChangeConsumerConfig.CLIENT_ID,
                ZerobusChangeConsumerConfig.CLIENT_SECRET,
                ZerobusChangeConsumerConfig.TABLE,
                ZerobusChangeConsumerConfig.MAX_INFLIGHT_RECORDS);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
