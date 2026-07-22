/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

/**
 * Quarkus test profile for JDBC sink integration tests with PostgreSQL source and target.
 */
public class JdbcPostgresTestProfile extends JdbcTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(JdbcPostgresResourceLifecycleManager.class));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = super.getConfigOverrides();
        config.put("debezium.source.schema.include.list", "inventory");
        return config;
    }
}
