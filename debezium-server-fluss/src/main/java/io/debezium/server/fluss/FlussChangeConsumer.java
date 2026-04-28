/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.server.util.RetryExecutor;

/**
 * An implementation of the {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} interface that publishes
 * change event messages to Apache Fluss.
 *
 * <p>Supports both primary-key tables (upsert/delete) and log tables (append only).
 * Requires the connector to be configured with schemas enabled.
 *
 * @author Chris Cranford
 */
@Named("fluss")
@Dependent
public class FlussChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlussChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.fluss.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();
    private final Map<TablePath, AppendWriter> appendWriters = new ConcurrentHashMap<>();
    private final Map<TablePath, UpsertWriter> upsertWriters = new ConcurrentHashMap<>();
    private final Map<TablePath, TableDescriptor> tableDescriptorCache = new ConcurrentHashMap<>();
    private final FlussTypeConverter typeConverter = new FlussTypeConverter();

    // These are visible for testing.
    // Can use DebeziumServerSink#configure in the future
    RetryExecutor retryExecutor;
    FlussChangeConsumerConfig config;
    Connection connection;
    Admin admin;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new FlussChangeConsumerConfig(configuration);

        Configuration flussConfig = new Configuration();
        flussConfig.setString("bootstrap.servers", config.getBootstrapServers());

        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();

        LOGGER.info("Connected to Fluss at '{}', default database: '{}'",
                config.getBootstrapServers(), config.getDefaultDatabase());

        retryExecutor = new RetryExecutor(
                config.getMaxRetries(),
                config.getRetryInterval().toMillis(),
                config.getRetryMaxInterval().toMillis(),
                config.getDefaultRetryBackoffMultiplier());
    }

    @PreDestroy
    @Override
    public void close() {
        appendWriters.clear();
        upsertWriters.clear();

        try {
            if (admin != null) {
                admin.close();
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception closing Fluss admin client", e);
        }

        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception closing Fluss connection", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        final Map<String, List<ChangeEvent<Object, Object>>> groupedByDestination = records.stream()
                .collect(Collectors.groupingBy(r -> streamNameMapper.map(r.destination())));

        for (Map.Entry<String, List<ChangeEvent<Object, Object>>> entry : groupedByDestination.entrySet()) {
            final String destination = entry.getKey();
            final TablePath tablePath = resolveTablePath(destination);
            final SourceRecord firstSourceRecord = toSourceRecord(entry.getValue().getFirst());
            final TableDescriptor descriptor = getOrFetchDescriptor(tablePath, firstSourceRecord);
            final boolean hasPrimaryKey = descriptor.hasPrimaryKey();

            for (ChangeEvent<Object, Object> record : entry.getValue()) {
                writeRecordWithRetry(record, tablePath, descriptor, hasPrimaryKey);
                committer.markProcessed(record);
            }

            flushWriters(tablePath, hasPrimaryKey);
        }

        committer.markBatchFinished();
    }

    // Not part of the public Debezium Engine API; Debezium Server may rely on this internal detail.
    @SuppressWarnings("rawtypes")
    protected SourceRecord toSourceRecord(ChangeEvent<Object, Object> record) {
        return ((EmbeddedEngineChangeEvent) record).sourceRecord();
    }

    private void writeRecordWithRetry(ChangeEvent<Object, Object> record, TablePath tablePath,
                                      TableDescriptor descriptor, boolean hasPrimaryKey)
            throws InterruptedException {
        retryExecutor.executeWithRetry(
                () -> {
                    writeRecord(record, tablePath, descriptor, hasPrimaryKey);
                    return null;
                },
                this::isRetriableFlussException,
                "Writing to Fluss " + tablePath);
    }

    private void writeRecord(ChangeEvent<Object, Object> record, TablePath tablePath,
                             TableDescriptor descriptor, boolean hasPrimaryKey) {

        final SourceRecord sourceRecord = toSourceRecord(record);
        if (sourceRecord.value() == null) {
            if (hasPrimaryKey) {
                LOGGER.debug("Received tombstone for {}, skipping (no before data available)", tablePath);
            }
            return;
        }

        final Schema valueSchema = sourceRecord.valueSchema();
        final Struct valueStruct = (Struct) sourceRecord.value();

        final String op;
        final Struct after;
        final Struct before;
        final Schema rowSchema;

        if (Envelope.isEnvelopeSchema(valueSchema)) {
            op = valueStruct.getString(Envelope.FieldName.OPERATION);
            after = valueStruct.getStruct(Envelope.FieldName.AFTER);
            before = valueStruct.getStruct(Envelope.FieldName.BEFORE);
            rowSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        }
        else {
            // Extracted new record state — whole struct is the row
            op = Envelope.Operation.CREATE.code();
            after = valueStruct;
            before = null;
            rowSchema = valueSchema;
        }

        final RowType rowType = descriptor.getSchema().getRowType();

        if (hasPrimaryKey) {
            final UpsertWriter writer = getOrCreateUpsertWriter(tablePath);
            if (Envelope.Operation.DELETE.code().equals(op) && before != null) {
                final Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
                writer.delete(buildRow(before, beforeSchema, rowType));
            }
            else if (after != null) {
                writer.upsert(buildRow(after, rowSchema, rowType));
            }
        }
        else {
            if (!Envelope.Operation.DELETE.code().equals(op) && after != null) {
                final AppendWriter writer = getOrCreateAppendWriter(tablePath);
                writer.append(buildRow(after, rowSchema, rowType));
            }
        }
    }

    private InternalRow buildRow(Struct data, Schema schema, RowType rowType) {
        final List<String> fieldNames = rowType.getFieldNames();
        final GenericRow row = new GenericRow(fieldNames.size());

        for (int i = 0; i < fieldNames.size(); i++) {
            final String name = fieldNames.get(i);
            final org.apache.kafka.connect.data.Field field = schema.field(name);
            final Object value = (field != null) ? data.get(field) : null;
            row.setField(i, typeConverter.toFlussValue(value, field != null ? field.schema() : null));
        }

        return row;
    }

    private TablePath resolveTablePath(String destination) {
        // Fluss table names allow only ASCII alphanumerics, '_', and '-'; replace other chars with '_'
        final String tableName = destination.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        return TablePath.of(config.getDefaultDatabase(), tableName);
    }

    private TableDescriptor getOrFetchDescriptor(TablePath tablePath, SourceRecord sourceRecord) {
        return tableDescriptorCache.computeIfAbsent(tablePath, path -> {
            try {
                if (config.isTableAutoCreate()) {
                    ensureTableExists(path, sourceRecord);
                }
                return admin.getTableInfo(path).get().toTableDescriptor();
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to fetch table descriptor for " + path, e);
            }
        });
    }

    private void ensureTableExists(TablePath tablePath, SourceRecord sourceRecord) throws Exception {
        final boolean exists = admin.tableExists(tablePath).get();
        if (exists) {
            return;
        }

        final Schema valueSchema = sourceRecord.valueSchema();
        if (valueSchema == null) {
            throw new DebeziumException("Cannot auto-create table " + tablePath
                    + ": event has no schema. Ensure the connector is configured with schemas.enable=true.");
        }

        final Schema afterSchema;
        if (Envelope.isEnvelopeSchema(valueSchema)) {
            final org.apache.kafka.connect.data.Field afterField = valueSchema.field(Envelope.FieldName.AFTER);
            afterSchema = (afterField != null) ? afterField.schema() : null;
        }
        else {
            afterSchema = valueSchema;
        }

        if (afterSchema == null) {
            throw new DebeziumException("Cannot auto-create table " + tablePath + ": no 'after' field schema found.");
        }

        final org.apache.fluss.metadata.Schema flussSchema = typeConverter.toFlussSchema(afterSchema, new ArrayList<>());
        final TableDescriptor descriptor = TableDescriptor.builder().schema(flussSchema).build();
        admin.createTable(tablePath, descriptor, true).get();
        LOGGER.info("Auto-created Fluss table {}", tablePath);
    }

    private AppendWriter getOrCreateAppendWriter(TablePath tablePath) {
        return appendWriters.computeIfAbsent(tablePath, path -> {
            try {
                Table table = connection.getTable(path);
                return table.newAppend().createWriter();
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to create append writer for " + path, e);
            }
        });
    }

    private UpsertWriter getOrCreateUpsertWriter(TablePath tablePath) {
        return upsertWriters.computeIfAbsent(tablePath, path -> {
            try {
                Table table = connection.getTable(path);
                return table.newUpsert().createWriter();
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to create upsert writer for " + path, e);
            }
        });
    }

    private void flushWriters(TablePath tablePath, boolean hasPrimaryKey) {
        try {
            if (hasPrimaryKey) {
                UpsertWriter writer = upsertWriters.get(tablePath);
                if (writer != null) {
                    writer.flush();
                }
            }
            else {
                AppendWriter writer = appendWriters.get(tablePath);
                if (writer != null) {
                    writer.flush();
                }
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to flush writer for " + tablePath, e);
        }
    }

    private boolean isRetriableFlussException(Exception e) {
        // Future extension point to control retriable/non-retriable exceptions
        // Right now, all exceptions are simply retried
        return true;
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                FlussChangeConsumerConfig.BOOTSTRAP_SERVERS,
                FlussChangeConsumerConfig.DEFAULT_DATABASE,
                FlussChangeConsumerConfig.TABLE_AUTO_CREATE,
                FlussChangeConsumerConfig.RETRIES_MAX,
                FlussChangeConsumerConfig.RETRIES_INTERVAL_MS,
                FlussChangeConsumerConfig.RETRIES_MAX_INTERVAL_MS,
                FlussChangeConsumerConfig.RETRIES_BACKOFF_MULTIPLIER);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
