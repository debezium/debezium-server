/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.data.Json;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.schema.SchemaFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.UpsertReq;

/**
 * Implementation of the consumer that delivers the messages into a Milvus vector database.
 *
 * @author Jiri Pechanec
 *
 */
@Named("milvus")
@Dependent
public class MilvusChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.milvus.";

    private MilvusClientV2 milvusClient;
    private MilvusSchema schema;
    private final Gson gson = new Gson();

    @ConfigProperty(name = PROP_PREFIX + "uri", defaultValue = "http://localhost:19530")
    String uri;

    @ConfigProperty(name = PROP_PREFIX + "database", defaultValue = "default")
    String databaseName;

    @ConfigProperty(name = PROP_PREFIX + "unwind.json", defaultValue = "false")
    boolean unwindJson;

    @Inject
    @CustomConsumerBuilder
    Instance<MilvusClientV2> customClient;

    @PostConstruct
    void connect() {
        if (customClient.isResolvable()) {
            milvusClient = customClient.get();
            LOGGER.info("Obtained custom configured MilvusClientV2 '{}'", milvusClient);
        }
        else {
            final var config = ConnectConfig.builder()
                    .uri(uri)
                    .build();
            milvusClient = new MilvusClientV2(config);
            schema = new MilvusSchema(milvusClient);
        }

        final var databases = milvusClient.listDatabases().getDatabaseNames();
        if (!databases.contains(databaseName)) {
            throw new DebeziumException(String.format("Database '%s' does not exist", databaseName));
        }
    }

    @PreDestroy
    void close() {
        try {
            milvusClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing client", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        for (final ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            final var sourceRecord = toSourceRecord(record);

            // Milvus does not support dots in collection names so we by default replace them
            // with underscores so the user does not need to provide router or mapper
            final var collectionName = streamNameMapper.map(record.destination()).replace('.', '_');

            if (isSchemaChange(sourceRecord)) {
                LOGGER.debug("Schema change event, ignoring it");
                committer.markProcessed(record);
                continue;
            }

            if (record.key() == null) {
                throw new DebeziumException("Milvus does not support collections without primary key");
            }
            schema.validateKey(collectionName, sourceRecord.keySchema());

            if (sourceRecord.value() == null) {
                deleteRecord(collectionName, record, committer);
            }
            else if (Envelope.isEnvelopeSchema(sourceRecord.valueSchema())) {
                final var valueStruct = (Struct) sourceRecord.value();
                switch (Operation.forCode(valueStruct.getString(Envelope.FieldName.OPERATION))) {
                    case Operation.READ:
                    case Operation.CREATE:
                    case Operation.UPDATE:
                        upsertRecord(collectionName, record, committer);
                        break;
                    case Operation.DELETE:
                        deleteRecord(collectionName, record, committer);
                        break;
                    default:
                        LOGGER.info("Unsupported operation, skipping record '{}'", record);
                }
            }
            else {
                // Extracted new record state
                upsertRecord(collectionName, record, committer);
            }
        }

        committer.markBatchFinished();
    }

    protected SourceRecord toSourceRecord(final ChangeEvent<Object, Object> record) {
        // Milvus sink requires access to the message schema so it can do schema evolution
        // This is not a part of public Debezium Engine API but is an implementation detail on
        // which Debezium Server can rely
        @SuppressWarnings("rawtypes")
        final var sourceRecord = ((EmbeddedEngineChangeEvent) record).sourceRecord();
        return sourceRecord;
    }

    private void upsertRecord(String collectionName, ChangeEvent<Object, Object> record,
                              RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final var data = getValue(record, toSourceRecord(record));

        final var request = UpsertReq.builder()
                .collectionName(collectionName)
                .data(List.of(data))
                .build();
        milvusClient.upsert(request);
        committer.markProcessed(record);
    }

    private void deleteRecord(String collectionName, ChangeEvent<Object, Object> record,
                              RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final var keyStruct = (Struct) toSourceRecord(record).key();
        final var key = keyStruct.get(keyStruct.schema().fields().get(0));

        final var request = DeleteReq.builder()
                .collectionName(collectionName)
                .ids(List.of(key))
                .build();
        milvusClient.delete(request);
        committer.markProcessed(record);
    }

    private JsonObject getValue(ChangeEvent<Object, Object> record, SourceRecord sourceRecord) {
        final var value = getString(record.value());
        var valueSchema = sourceRecord.valueSchema();

        var json = gson.fromJson(value, JsonObject.class);

        if ((json.has("schema") || json.has("schemaId")) && json.has("payload")) {
            // JSON serialized message with schema
            json = json.getAsJsonObject("payload");
        }

        if (Envelope.isEnvelopeSchema(sourceRecord.valueSchema())) {
            // Message is envelope, so only after part is used
            json = json.getAsJsonObject(Envelope.FieldName.AFTER);
            valueSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        }

        if (unwindJson) {
            for (Field field : valueSchema.fields()) {
                if (Json.LOGICAL_NAME.equals(field.schema().name()) && json.has(field.name())) {
                    final var stringValue = json.get(field.name()).getAsString();
                    final var jsonValue = gson.fromJson(stringValue, JsonObject.class);
                    json.add(field.name(), jsonValue);
                }
            }
        }
        return json;
    }

    private boolean isSchemaChange(final SourceRecord record) {
        return record.valueSchema() != null && record.valueSchema().name() != null
                && SchemaFactory.get().isSchemaChangeSchema(record.valueSchema());
    }
}
