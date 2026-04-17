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

/**
 * Test configuration for JDBC sink integration tests.
 */
public class JdbcTestConfigSource extends TestConfigSource {

    public JdbcTestConfigSource() {
        Map<String, String> jdbcTest = new HashMap<>();

        // Sink configuration
        jdbcTest.put("debezium.sink.type", "jdbc");

        // JDBC sink connection - will be overridden by test resource lifecycle manager
        jdbcTest.put("debezium.sink.jdbc.connection.url", "jdbc:postgresql://localhost:5432/postgres");
        jdbcTest.put("debezium.sink.jdbc.connection.username", "postgres");
        jdbcTest.put("debezium.sink.jdbc.connection.password", "postgres");

        // JDBC sink behavior
        jdbcTest.put("debezium.sink.jdbc.insert.mode", "upsert");
        jdbcTest.put("debezium.sink.jdbc.primary.key.mode", "record_key");
        jdbcTest.put("debezium.sink.jdbc.schema.evolution", "basic");
        jdbcTest.put("debezium.sink.jdbc.delete.enabled", "true");
        jdbcTest.put("debezium.sink.jdbc.batch.size", "100");

        // Source configuration
        jdbcTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        jdbcTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        jdbcTest.put("debezium.source.offset.flush.interval.ms", "0");
        jdbcTest.put("debezium.source.topic.prefix", "testserver");
        jdbcTest.put("debezium.source.schema.include.list", "inventory");
        jdbcTest.put("debezium.source.table.include.list", "inventory.customers,inventory.products");

        config = jdbcTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
