/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.schema.SchemaFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.PointStruct;

/**
 * Implementation of the consumer that delivers the messages into a Qdrant
 * vector database.
 *
 * @author Jiri Pechanec
 *
 */
@Named("qdrant")
@Dependent
public class QdrantChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QdrantChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.qdrant.";

    private QdrantClient qdrantClient;
    private QdrantMessageFactory messageFactory;

    @ConfigProperty(name = PROP_PREFIX + "host", defaultValue = "localhost")
    String host;

    @ConfigProperty(name = PROP_PREFIX + "port", defaultValue = "6333")
    int port;

    @ConfigProperty(name = PROP_PREFIX + "api.key")
    Optional<String> apiKey;

    @Inject
    @CustomConsumerBuilder
    Instance<QdrantClient> customClient;

    @PostConstruct
    void connect() {
        if (customClient.isResolvable()) {
            qdrantClient = customClient.get();
            LOGGER.info("Obtained custom configured QdrantClient '{}'", qdrantClient);
        }
        else {
            var qdrantGrpcChannelBuilder = Grpc.newChannelBuilder("%s:%s".formatted(host, port),
                    InsecureChannelCredentials.create());
            var qdrantClientBuilder = QdrantGrpcClient.newBuilder(qdrantGrpcChannelBuilder.build(), true);
            if (apiKey.isPresent()) {
                qdrantClientBuilder = qdrantClientBuilder.withApiKey(apiKey.get());
            }
            qdrantClient = new QdrantClient(qdrantClientBuilder.build());
        }

        messageFactory = new QdrantMessageFactory();
    }

    @PreDestroy
    void close() {
        try {
            qdrantClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing client", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        for (final ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            final var collectionName = streamNameMapper.map(record.destination());

            final var sourceRecord = toSourceRecord(record);

            if (isSchemaChange(sourceRecord)) {
                LOGGER.debug("Schema change event, ignoring it");
                committer.markProcessed(record);
                continue;
            }

            if (record.key() == null) {
                throw new DebeziumException("Qdrant does not support collections without primary key");
            }

            if (sourceRecord.value() == null) {
                deleteRecord(collectionName, record, committer);
            }
            else if (Envelope.isEnvelopeSchema(sourceRecord.valueSchema())) {
                final var valueStruct = (Struct) sourceRecord.value();
                final var payload = ((Struct) sourceRecord.value()).getStruct(Envelope.FieldName.AFTER);
                switch (Operation.forCode(valueStruct.getString(Envelope.FieldName.OPERATION))) {
                    case Operation.READ:
                    case Operation.CREATE:
                    case Operation.UPDATE:
                        upsertRecord(collectionName, record, payload, committer);
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
                final var payload = (Struct) sourceRecord.value();
                upsertRecord(collectionName, record, payload, committer);
            }
        }

        committer.markBatchFinished();
    }

    protected SourceRecord toSourceRecord(final ChangeEvent<Object, Object> record) {
        // Qdrant sink requires access to the message schema so it can process
        // the fields individually based on their type
        // This is not a part of public Debezium Engine API but is an
        // implementation detail on which Debezium Server can rely
        @SuppressWarnings("rawtypes")
        final var sourceRecord = ((EmbeddedEngineChangeEvent) record).sourceRecord();
        return sourceRecord;
    }

    private boolean isSchemaChange(final SourceRecord record) {
        return record.valueSchema() != null && record.valueSchema().name() != null
                && SchemaFactory.get().isSchemaChangeSchema(record.valueSchema());
    }

    private void upsertRecord(String collectionName, ChangeEvent<Object, Object> record, Struct payload,
                              RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final var key = ((Struct) toSourceRecord(record).key());

        final var point = PointStruct.newBuilder().setId(messageFactory.toPointId(key))
                .setVectors(messageFactory.toVectors(collectionName, payload))
                .putAllPayload(messageFactory.toPayloadMap(collectionName, key, payload)).build();

        try {
            final var updateResult = qdrantClient.upsertAsync(collectionName, List.of(point)).get();
        }
        catch (ExecutionException e) {
            throw new DebeziumException("Error while upserting data into Qdrant", e.getCause());
        }

        committer.markProcessed(record);
    }

    private void deleteRecord(String collectionName, ChangeEvent<Object, Object> record,
                              RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final var key = ((Struct) toSourceRecord(record).key());

        try {
            final var deleteResult = qdrantClient.deleteAsync(collectionName, List.of(messageFactory.toPointId(key))).get();
        }
        catch (ExecutionException e) {
            throw new DebeziumException("Error while deleteing data from Qdrant", e.getCause());
        }

        committer.markProcessed(record);
    }

}
