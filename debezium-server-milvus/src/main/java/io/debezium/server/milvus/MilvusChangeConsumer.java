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
import jakarta.inject.Named;

import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
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

    @PostConstruct
    void connect() {
        final var config = ConnectConfig.builder()
                .uri(uri)
                .build();
        milvusClient = new MilvusClientV2(config);
        schema = new MilvusSchema(milvusClient);

        final var databases = milvusClient.listDatabases().getDatabaseNames();
        if (!databases.contains(databaseName)) {
            LOGGER.info("Database '{}' is not available, trying to create it");
            final var request = CreateDatabaseReq.builder()
                    .databaseName(databaseName)
                    .build();
            milvusClient.createDatabase(request);
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

            // Milvus sink requires access to the message schema so it can do schema evolution
            // This is not a part of public Debezium Engine API but is an implementation detail on
            // which Debezium Server can rely
            @SuppressWarnings("rawtypes")
            final var sourceRecord = ((EmbeddedEngineChangeEvent) record).sourceRecord();

            // Milvus does not support dots in collection names so we by default replace them
            // with underscores so the user does not need to provide router or mapper
            final var collectionName = streamNameMapper.map(record.destination()).replace('.', '_');

            if (record.key() == null) {
                throw new DebeziumException("Milvus does not support collections without primary key");
            }
            schema.validateKey(sourceRecord.keySchema());

            final var data = getData(record, sourceRecord);

            final var request = UpsertReq.builder()
                    .collectionName(collectionName)
                    .data(List.of(data))
                    .build();
            milvusClient.upsert(request);
            committer.markProcessed(record);
        }

        committer.markBatchFinished();
    }

    private JsonObject getData(ChangeEvent<Object, Object> record, SourceRecord sourceRecord) {
        final var value = getString(record.value());
        var json = gson.fromJson(value, JsonObject.class);

        if ((json.has("schema") || json.has("schemaId")) && json.has("payload")) {
            // JSON serialized message with schema
            json = json.getAsJsonObject("payload");
        }

        if (Envelope.isEnvelopeSchema(sourceRecord.valueSchema())) {
            // Message is envelope, so only after part is used
            json = json.getAsJsonObject(Envelope.FieldName.AFTER);
        }

        return json;
    }
}
