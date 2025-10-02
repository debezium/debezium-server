/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED;
import static io.debezium.openlineage.dataset.DatasetMetadata.STREAM_DATASET_TYPE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DataStore.KAFKA;
import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.OUTPUT;
import static io.debezium.server.DebeziumServer.PROP_SINK_TYPE;
import static io.debezium.server.DebeziumServer.PROP_SOURCE_PREFIX;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetDataExtractor;
import io.debezium.openlineage.dataset.DatasetMetadata;

public class DefaultChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultChangeConsumer.class);

    private final DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> delegateConsumer;
    private final Config config;
    private final DatasetDataExtractor datasetDataExtractor;
    private final boolean isOpenLineageEnabled;

    public DefaultChangeConsumer(DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> delegateConsumer, Config config) {
        this.delegateConsumer = delegateConsumer;
        this.config = config;
        this.datasetDataExtractor = new DatasetDataExtractor();
        this.isOpenLineageEnabled = config.getOptionalValue(PROP_SOURCE_PREFIX + OPEN_LINEAGE_INTEGRATION_ENABLED, boolean.class).orElse(false);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        final String sink = config.getValue(PROP_SINK_TYPE, String.class);

        try {
            this.delegateConsumer.handleBatch(records, committer);
        }
        catch (Exception e) {
            throw new DebeziumException("Error while executing batch", e);
        }

        if (isOpenLineageEnabled) {
            Optional<DatasetMetadata.DataStore> dataStore = getDataStore(sink);
            dataStore.ifPresentOrElse(
                    extractLineageMetadata(records),
                    () -> LOGGER.warn("Current {} sink is not supported to emit OpenLineage output dataset", sink));
        }

    }

    private Consumer<DatasetMetadata.DataStore> extractLineageMetadata(List<ChangeEvent<Object, Object>> records) {
        return store -> records.forEach(record -> {
            SourceRecord sourceRecord = ((EmbeddedEngineChangeEvent<Object, Object, Object>) record).sourceRecord();
            ConnectHeaders headers = new ConnectHeaders();

            convertHeaders(record).forEach(headers::addString);
            ConnectorContext connectorContext = ConnectorContext.from(sourceRecord.headers());
            String datasetName = streamNameMapper.map(record.destination());

            List<DatasetMetadata.FieldDefinition> fieldDefinitions = datasetDataExtractor
                    .extract(sourceRecord);
            DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING,
                    List.of(new DatasetMetadata(datasetName, OUTPUT, STREAM_DATASET_TYPE, store, fieldDefinitions)));
        });
    }

    private Optional<DatasetMetadata.DataStore> getDataStore(String sink) {
        return switch (sink) {
            case "kafka" -> Optional.of(KAFKA);
            default -> Optional.empty();
        };
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return this.delegateConsumer.supportsTombstoneEvents();
    }

    public DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> getDelegateConsumer() {
        return this.delegateConsumer;
    }
}
