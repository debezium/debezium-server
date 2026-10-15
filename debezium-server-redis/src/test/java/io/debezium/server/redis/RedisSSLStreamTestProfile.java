/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisSSLStreamTestProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(
                new TestResourceEntry(RedisSSLSystemPropertiesTestResourceLifecycleManager.class),
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(RedisSSLTestResourceLifecycleManager.class));
    }

    public Map<String, String> getConfigOverrides() {

        Map<String, String> config = new HashMap<String, String>();
        config.put("debezium.source.offset.storage", "redis");
        config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        return config;
    }

}
