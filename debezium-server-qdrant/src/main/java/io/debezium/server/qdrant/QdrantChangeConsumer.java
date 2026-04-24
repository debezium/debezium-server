/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.schema.SchemaFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.api.DebeziumServerSink;
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
        implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(QdrantChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.qdrant.";
    private static final String FIELD_INCLUDE_LIST_PROP_PREFIX = PROP_PREFIX + "field.include.list.";

    private QdrantChangeConsumerConfig config;
    private QdrantClient qdrantClient;
    private QdrantMessageFactory messageFactory;

    @Inject
    @CustomConsumerBuilder
    Instance<QdrantClient> customClient;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new QdrantChangeConsumerConfig(configuration);

        if (customClient.isResolvable()) {
            qdrantClient = customClient.get();
            LOGGER.info("Obtained custom configured QdrantClient '{}'", qdrantClient);
        }
        else {
            var qdrantGrpcChannelBuilder = Grpc.newChannelBuilder("%s:%s".formatted(config.getHost(), config.getPort()),
                    InsecureChannelCredentials.create());
            var qdrantClientBuilder = QdrantGrpcClient.newBuilder(qdrantGrpcChannelBuilder.build(), true);
            if (config.getApiKey() != null) {
                qdrantClientBuilder = qdrantClientBuilder.withApiKey(config.getApiKey());
            }
            qdrantClient = new QdrantClient(qdrantClientBuilder.build());
            LOGGER.info("Created standard QdrantClient '{}'", qdrantClient);
        }

        messageFactory = new QdrantMessageFactory(
                config.getVectorFieldNames() != null ? Optional.of(config.getVectorFieldNames()) : Optional.empty(),
                getConfigSubset(mpConfig, FIELD_INCLUDE_LIST_PROP_PREFIX));
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            qdrantClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing client", e);
        }
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events)
            throws InterruptedException {

        for (final BatchEvent record : events.records()) {
            LOGGER.trace("Received event '{}'", record);

            final var collectionName = streamNameMapper.map(events.destination());

            if (isSchemaChange(record.record())) {
                LOGGER.debug("Schema change event, ignoring it");
                record.commit();
                continue;
            }

            if (record.key() == null) {
                throw new DebeziumException("Qdrant does not support collections without primary key");
            }

            if (record.record().value() == null) {
                deleteRecord(collectionName, record);
            }
            else if (Envelope.isEnvelopeSchema(record.record().valueSchema())) {
                final var valueStruct = (Struct) record.record().value();
                final var payload = ((Struct) record.record().value()).getStruct(Envelope.FieldName.AFTER);
                switch (Operation.forCode(valueStruct.getString(Envelope.FieldName.OPERATION))) {
                    case Operation.READ:
                    case Operation.CREATE:
                    case Operation.UPDATE:
                        upsertRecord(collectionName, record, payload);
                        break;
                    case Operation.DELETE:
                        deleteRecord(collectionName, record);
                        break;
                    default:
                        LOGGER.info("Unsupported operation, skipping record '{}'", record);
                }
            }
            else {
                // Extracted new record state
                final var payload = (Struct) record.record().value();
                upsertRecord(collectionName, record, payload);
            }
        }

    }

    private boolean isSchemaChange(final SourceRecord record) {
        return record.valueSchema() != null && record.valueSchema().name() != null
                && SchemaFactory.get().isSchemaChangeSchema(record.valueSchema());
    }

    private void upsertRecord(String collectionName, BatchEvent record, Struct payload)
            throws InterruptedException {
        final var key = ((Struct) record.record().key());

        final var point = PointStruct.newBuilder().setId(messageFactory.toPointId(key))
                .setVectors(messageFactory.toVectors(collectionName, payload))
                .putAllPayload(messageFactory.toPayloadMap(collectionName, key, payload)).build();

        try {
            final var updateResult = qdrantClient.upsertAsync(collectionName, List.of(point)).get();
        }
        catch (ExecutionException e) {
            throw new DebeziumException("Error while upserting data into Qdrant", e.getCause());
        }

        record.commit();
    }

    private void deleteRecord(String collectionName, BatchEvent record)
            throws InterruptedException {
        final var key = ((Struct) record.record().key());

        try {
            final var deleteResult = qdrantClient.deleteAsync(collectionName, List.of(messageFactory.toPointId(key))).get();
        }
        catch (ExecutionException e) {
            throw new DebeziumException("Error while deleteing data from Qdrant", e.getCause());
        }

        record.commit();
    }

    @Override
    public io.debezium.config.Field.Set getConfigFields() {
        return io.debezium.config.Field.setOf(
                QdrantChangeConsumerConfig.HOST,
                QdrantChangeConsumerConfig.PORT,
                QdrantChangeConsumerConfig.API_KEY,
                QdrantChangeConsumerConfig.VECTOR_FIELD_NAMES);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }

}
