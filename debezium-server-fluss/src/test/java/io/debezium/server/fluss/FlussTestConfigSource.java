/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

/**
 * Apache Fluss test suite configuration.
 *
 * @author Chris Cranford
 */
public class FlussTestConfigSource extends TestConfigSource {

    public static final String DEFAULT_DATABASE = "fluss";

    public FlussTestConfigSource() {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("debezium.sink.type", "fluss");
        configMap.put("debezium.sink.fluss.default.database", DEFAULT_DATABASE);
        configMap.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        configMap.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        configMap.put("debezium.source.offset.flush.interval.ms", "0");
        configMap.put("debezium.source.topic.prefix", "testc");
        configMap.put("debezium.source.schema.include.list", "inventory");
        configMap.put("debezium.source.table.include.list", "inventory.customers");

        config = configMap;
    }

    @Override
    public int getOrdinal() {
        return super.getOrdinal() + 1;
    }

    public static int waitForSeconds() {
        return 60;
    }
}