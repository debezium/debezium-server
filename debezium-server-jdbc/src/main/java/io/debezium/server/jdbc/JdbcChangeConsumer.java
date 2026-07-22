/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.Collection;
import java.util.HashMap;
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
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorTask;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerConsumer;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Implementation of the consumer that delivers change events to a JDBC database.
 * <p>
 * Delegates to {@link JdbcSinkConnectorTask} for all JDBC sink logic including
 * initialization, dialect resolution, record writer selection, and shutdown.
 * This ensures the Debezium Server JDBC sink always stays in sync with the
 * Kafka Connect JDBC sink connector.
 *
 * @author Mario Fiore Vitale
 * @author rk3rn3r
 */
@Named("jdbc")
@Dependent
public class JdbcChangeConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.jdbc.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();
    private final ChangeEventToSinkRecordConverter converter = new ChangeEventToSinkRecordConverter();

    private JdbcSinkConnectorTask task;

    @PostConstruct
    void connect() {
        LOGGER.info("Initializing JDBC sink");

        try {
            final Config mpConfig = ConfigProvider.getConfig();
            final Map<String, String> props = toStringMap(getConfigSubset(mpConfig, PROP_PREFIX));

            this.task = new JdbcSinkConnectorTask();
            task.start(props);

            LOGGER.info("JDBC sink initialized successfully");
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize JDBC sink", e);
            close();
            throw new DebeziumException("Failed to initialize JDBC sink", e);
        }
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {

        LOGGER.debug("Processing batch of {} records", events.records().size());

        final Collection<SinkRecord> sinkRecords = events.records().stream()
                .map(converter::convert)
                .collect(Collectors.toList());

        task.put(sinkRecords);

        final Throwable exception = task.getLastProcessingException();
        if (exception != null) {
            throw new DebeziumException("Failed to process batch", exception);
        }

        for (BatchEvent record : events.records()) {
            record.commit();
        }

        LOGGER.debug("Successfully processed batch of {} records", events.records().size());
    }

    @PreDestroy
    @Override
    public void close() {
        LOGGER.info("Closing JDBC sink");

        if (task != null) {
            try {
                task.stop();
            }
            catch (Exception e) {
                LOGGER.warn("Error stopping JDBC sink task", e);
            }
            finally {
                task = null;
            }
        }

        LOGGER.info("JDBC sink closed");
    }

    @Override
    public Field.Set getConfigFields() {
        return JdbcSinkConnectorConfig.ALL_FIELDS;
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }

    private static Map<String, String> toStringMap(Map<String, Object> source) {
        final Map<String, String> result = new HashMap<>();
        source.forEach((key, value) -> {
            if (value != null) {
                result.put(key, value.toString());
            }
        });
        return result;
    }
}
