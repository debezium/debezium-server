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

public class RedisOffsetTestProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(RedisTestResourceLifecycleManager.class,
                        Map.of(
                                "debezium.source.offset.storage.redis.address", "address",
                                "debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector")));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<>();
        config.put("debezium.sink.type", "redis");
        config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore");
        config.put("debezium.source.offset.flush.interval.ms", "0");

        return config;
    }

}
