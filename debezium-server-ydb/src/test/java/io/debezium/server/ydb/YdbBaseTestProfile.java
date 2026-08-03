/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public abstract class YdbBaseTestProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(YdbTestResourceLifecycleManager.class));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of();
    }
}
