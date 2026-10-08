/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorTask;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerConsumer;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Implementation of the consumer that delivers change events to Elasticsearch.
 * <p>
 * Delegates to {@link ElasticsearchSinkConnectorTask} for all sink logic, including client
 * construction and the cluster version handshake, mapping management, bulk writes, and shutdown.
 * This keeps the Debezium Server Elasticsearch sink in sync with the Kafka Connect Elasticsearch
 * sink connector, as DDD-61 section 14 requires.
 * <p>
 * The consumer hands the task the engine's original Connect record, so the documents written do
 * not depend on the {@code debezium.format.*} serialization settings.
 *
 * @author Chris Cranford
 */
@Named("elasticsearch")
@Dependent
public class ElasticsearchChangeConsumer extends BaseChangeConsumer
        implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.elasticsearch.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();
    private final ChangeEventToSinkRecordConverter converter = new ChangeEventToSinkRecordConverter();

    private ElasticsearchSinkConnectorTask task;

    @PostConstruct
    void connect() {
        LOGGER.info("Initializing Elasticsearch sink");
        try {
            final Config mpConfig = ConfigProvider.getConfig();
            final Map<String, String> props = toStringMap(getConfigSubset(mpConfig, PROP_PREFIX));

            this.task = new ElasticsearchSinkConnectorTask();
            task.start(props);

            LOGGER.info("Elasticsearch sink initialized successfully");
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize Elasticsearch sink", e);
            close();
            throw new DebeziumException("Failed to initialize Elasticsearch sink", e);
        }
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {
        LOGGER.debug("Processing batch of {} records", events.records().size());

        final Collection<SinkRecord> sinkRecords = events.records().stream()
                .map(converter::convert)
                .collect(Collectors.toList());

        DebeziumException putException = null;
        try {
            task.put(sinkRecords);
        }
        catch (Exception e) {
            LOGGER.error("ElasticsearchSinkTask failed", e);
            putException = new DebeziumException("ElasticsearchSinkTask failed", e);
        }
        finally {
            // The task does not propagate a write failure from put; it records it and rethrows
            // on the next call. Read it here so the batch is never committed past a failed write.
            final Throwable lastException = task.getLastProcessingException();
            if (lastException != null) {
                if (putException != null) {
                    putException.addSuppressed(lastException);
                }
                else {
                    putException = new DebeziumException("Failed to process batch", lastException);
                }
            }
        }
        if (putException != null) {
            close();
            throw putException;
        }

        for (BatchEvent record : events.records()) {
            record.commit();
        }

        LOGGER.debug("Successfully processed batch of {} records", events.records().size());
    }

    @PreDestroy
    @Override
    public void close() {
        LOGGER.info("Closing Elasticsearch sink");
        if (task != null) {
            try {
                task.stop();
            }
            catch (Exception e) {
                LOGGER.warn("Error stopping Elasticsearch sink task", e);
            }
            finally {
                task = null;
            }
        }
        LOGGER.info("Elasticsearch sink closed");
    }

    @Override
    public Field.Set getConfigFields() {
        return ElasticsearchSinkConnectorConfig.ALL_FIELDS;
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }

    private static Map<String, String> toStringMap(Map<String, Object> source) {
        return source.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }
}
