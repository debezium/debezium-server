/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.eclipse.microprofile.config.spi.ConfigSource;

import io.debezium.data.Json;
import io.debezium.util.Testing;

/**
 * A config source used during tests. Amended/overridden by values exposed from test lifecycle listeners.
 */
public class TestConfigSource implements ConfigSource {

    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();

    final Map<String, String> config = new HashMap<>();

    public TestConfigSource() {

        config.put("debezium.sink.type", "test");
        config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        config.put("debezium.source.offset.flush.interval.ms", "0");
        config.put("debezium.source.topic.prefix", "testc");
        config.put("debezium.source.schema.include.list", "inventory");
        config.put("debezium.source.table.include.list", "inventory.customers");

        String format = System.getProperty("test.apicurio.converter.format");
        String formatKey = System.getProperty("debezium.format.key");
        String formatValue = System.getProperty("debezium.format.value");
        String formatHeader = System.getProperty("debezium.format.header", "json");

        if (format != null && format.length() != 0) {
            config.put("debezium.format.key", format);
            config.put("debezium.format.value", format);
            config.put("debezium.format.header", formatHeader);
            // TODO remove once https://github.com/Apicurio/apicurio-registry/issues/4351 is fixed
            config.put("debezium.source.record.processing.threads", "1");
        }
        else {
            formatKey = (formatKey != null) ? formatKey : Json.class.getSimpleName().toLowerCase();
            formatValue = (formatValue != null) ? formatValue : Json.class.getSimpleName().toLowerCase();
            formatHeader = (formatHeader != null) ? formatHeader : Json.class.getSimpleName().toLowerCase();
            config.put("debezium.format.key", formatKey);
            config.put("debezium.format.value", formatValue);
            config.put("debezium.format.header", formatHeader);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return config;
    }

    @Override
    public String getValue(String propertyName) {
        return config.get(propertyName);
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public Set<String> getPropertyNames() {
        return config.keySet();
    }

    public static int waitForSeconds() {
        return 60;
    }
}
