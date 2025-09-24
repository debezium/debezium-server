/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetDataExtractor;
import io.debezium.openlineage.dataset.DatasetMetadata;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;

import static io.debezium.openlineage.dataset.DatasetMetadata.DataStore.KAFKA;
import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.OUTPUT;
import static io.debezium.openlineage.dataset.DatasetMetadata.STREAM_DATASET_TYPE;

/**
 * Basic services provided to all change consumers.
 *
 * @author Jiri Pechanec
 *
 */
public abstract class BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseChangeConsumer.class);

    protected StreamNameMapper streamNameMapper = (x) -> x;

    @Inject
    Instance<StreamNameMapper> customStreamNameMapper;
    private Config config;
    private DatasetDataExtractor datasetDataExtractor;

    @PostConstruct
    void init() {
        if (customStreamNameMapper.isResolvable()) {
            streamNameMapper = customStreamNameMapper.get();
        }
        LOGGER.info("Using '{}' stream name mapper", streamNameMapper);
        config = ConfigProvider.getConfig();
        datasetDataExtractor = new DatasetDataExtractor();
    }

    /**
     * Get a subset of the configuration properties that matches the given prefix.
     *
     * @param config    The global configuration object to extract the subset from.
     * @param prefix    The prefix to filter property names.
     *
     * @return          A subset of the original configuration properties containing property names
     *                  without the prefix.
     */
    protected Map<String, Object> getConfigSubset(Config config, String prefix) {
        final Map<String, Object> ret = new HashMap<>();

        for (String propName : config.getPropertyNames()) {
            if (propName.startsWith(prefix)) {
                final String newPropName = propName.substring(prefix.length());
                ret.put(newPropName, config.getConfigValue(propName).getValue());
            }
        }

        return ret;
    }

    protected byte[] getBytes(Object object) {
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        else if (object instanceof String) {
            return ((String) object).getBytes(StandardCharsets.UTF_8);
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    protected String getString(Object object) {
        if (object instanceof String) {
            return (String) object;
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    protected String asString(Object object) {
        return switch (object) {
            case null -> "null";
            case String s -> s;
            case byte[] b -> new String(b, StandardCharsets.UTF_8);
            default -> object.toString();
        };
    }

    protected String unsupportedTypeMessage(Object object) {
        final String type = (object == null) ? "null" : object.getClass().getName();
        return "Unexpected data type '" + type + "'";
    }

    protected Map<String, String> convertHeaders(ChangeEvent<Object, Object> record) {
        List<Header<Object>> headers = record.headers();
        Map<String, String> result = new HashMap<>();
        for (Header<Object> header : headers) {
            result.put(header.getKey(), getString(header.getValue()));
        }
        return result;
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        final String sink = config.getValue("debezium.sink.type", String.class);

        try {
            consumeBatch(records, committer);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        records.forEach(record-> {
            ConnectHeaders headers = new ConnectHeaders();

            convertHeaders(record).forEach(headers::addString);
            ConnectorContext connectorContext = ConnectorContext.from(headers);
            String theName = "the name"; //TODO resolve based on the sink type
            DatasetMetadata.DataStore dataStore = KAFKA; //TODO revolve based on the sink type

            List<DatasetMetadata.FieldDefinition> fieldDefinitions = datasetDataExtractor.extract(((EmbeddedEngineChangeEvent<Object, Object, Object>) record).sourceRecord());
            DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING,
                    List.of(new DatasetMetadata(theName, OUTPUT, STREAM_DATASET_TYPE, dataStore, fieldDefinitions)));
        });
    }

    public abstract void consumeBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) throws Exception;
}
