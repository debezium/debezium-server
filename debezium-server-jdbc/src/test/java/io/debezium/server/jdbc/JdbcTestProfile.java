/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;
import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Base Quarkus test profile for JDBC sink integration tests.
 * Provides shared JDBC sink configuration. Subclasses define the
 * source and target test resources for each database combination.
 */
public abstract class JdbcTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<>();

        // Sink type
        config.put("debezium.sink.type", "jdbc");

        // JDBC sink behavior
        config.put("debezium.sink.jdbc.insert.mode", "upsert");
        config.put("debezium.sink.jdbc.primary.key.mode", "record_key");
        config.put("debezium.sink.jdbc.schema.evolution", "basic");
        config.put("debezium.sink.jdbc.delete.enabled", "true");
        config.put("debezium.sink.jdbc.batch.size", "100");

        // Disable StarRocks dialect resolver to avoid Quarkus classloader conflict
        config.put("debezium.sink.jdbc.hibernate.dialect_resolvers", "");

        // Source configuration
        config.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                TestConfigSource.OFFSET_STORE_PATH.toAbsolutePath().toString());
        config.put("debezium.source.offset.flush.interval.ms", "0");
        config.put("debezium.source.topic.prefix", "testserver");
        config.put("debezium.source.table.include.list", "inventory.customers,inventory.products");
        config.put("debezium.source.schema.include.list", "");

        return config;
    }
}
