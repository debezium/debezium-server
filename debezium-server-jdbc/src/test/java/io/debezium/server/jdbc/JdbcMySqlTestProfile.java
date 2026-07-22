/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.MySqlTestResourceLifecycleManager;
import io.debezium.util.Testing;

/**
 * Quarkus test profile for JDBC sink integration tests with MySQL source and target.
 */
public class JdbcMySqlTestProfile extends JdbcTestProfile {

    public static final String SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("schema-history.dat").toAbsolutePath().toString();

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(
                new TestResourceEntry(MySqlTestResourceLifecycleManager.class),
                new TestResourceEntry(JdbcMySqlResourceLifecycleManager.class));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = super.getConfigOverrides();

        config.put("debezium.source.database.server.id", "12345");
        config.put("debezium.source.snapshot.locking.mode", "none");
        config.put("debezium.source.schema.history.internal",
                "io.debezium.storage.file.history.FileSchemaHistory");
        config.put("debezium.source.schema.history.internal.file.filename", SCHEMA_HISTORY_PATH);
        config.put("debezium.source.database.include.list", "inventory");

        return config;
    }
}
