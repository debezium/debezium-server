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
import static io.debezium.server.configuration.DebeziumProperties.PROP_SINK_TYPE;
import static io.debezium.server.configuration.DebeziumProperties.PROP_SOURCE_PREFIX;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.engine.StopEngineException;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetDataExtractor;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.ChangeConsumerHolder;
import io.debezium.server.configuration.DebeziumServerConfig;
import io.debezium.server.lifecycle.ShutdownAfterSnapshotGuard;
import io.quarkus.runtime.Startup;

@ApplicationScoped
@Startup
public class DefaultChangeConsumer extends BaseChangeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultChangeConsumer.class);

    private final ChangeConsumerHolder<CapturingEvents<BatchEvent>> delegateConsumer;
    private final Config config;
    private final DatasetDataExtractor datasetDataExtractor;
    private final boolean isOpenLineageEnabled;
    private final ShutdownAfterSnapshotGuard shutdownAfterSnapshotGuard;

    @Inject
    public DefaultChangeConsumer(ChangeConsumerHolder<CapturingEvents<BatchEvent>> delegateConsumer, Config config,
                                 DebeziumServerConfig serverConfig) {
        this.delegateConsumer = delegateConsumer;
        this.config = config;
        this.datasetDataExtractor = new DatasetDataExtractor();
        this.isOpenLineageEnabled = config.getOptionalValue(PROP_SOURCE_PREFIX + OPEN_LINEAGE_INTEGRATION_ENABLED, boolean.class).orElse(false);
        if (serverConfig.shutdownAfterSnapshot()) {
            ShutdownAfterSnapshotGuard.requireSupportedConfiguration(
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "snapshot.mode", String.class).orElse("initial"),
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "notification.enabled.channels", String.class).orElse(null),
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "tasks.max", Integer.class).orElse(1),
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "database.names", String.class).orElse(null),
                    serverConfig.shutdownOnCompletion());
            this.shutdownAfterSnapshotGuard = new ShutdownAfterSnapshotGuard();
        }
        else {
            this.shutdownAfterSnapshotGuard = null;
        }
    }

    @Capturing
    public void handleBatch(CapturingEvents<BatchEvent> events)
            throws InterruptedException {

        final String sink = config.getValue(PROP_SINK_TYPE, String.class);

        try {
            this.delegateConsumer.get().handle(events);
        }
        catch (Exception e) {
            throw new DebeziumException("Error while executing batch", e);
        }

        if (isOpenLineageEnabled) {
            Optional<DatasetMetadata.DataStore> dataStore = getDataStore(sink);
            dataStore.ifPresentOrElse(
                    extractLineageMetadata(events),
                    () -> LOGGER.warn("Current {} sink is not supported to emit OpenLineage output dataset", sink));
        }

        // Only after the sink handled this batch: the engine stops here and commits the offsets of the delivered records,
        // including the one of the notification, which is the offset of the completed snapshot.
        if (shutdownAfterSnapshotGuard != null
                && events.records().stream().anyMatch(record -> shutdownAfterSnapshotGuard.isEndOfInitialSnapshot(record.record()))) {
            LOGGER.info("Initial snapshot delivered to the sink, stopping the engine");
            throw new StopEngineException("Initial snapshot delivered, stopping the engine");
        }

    }

    private Consumer<DatasetMetadata.DataStore> extractLineageMetadata(CapturingEvents<BatchEvent> events) {
        return store -> events.records().forEach(record -> {
            ConnectHeaders headers = new ConnectHeaders();

            convertHeaders(record).forEach(headers::addString);
            ConnectorContext connectorContext = ConnectorContext.from(record.record().headers());
            String datasetName = streamNameMapper.map(record.destination());

            List<DatasetMetadata.FieldDefinition> fieldDefinitions = datasetDataExtractor
                    .extract(record.record());
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

}
