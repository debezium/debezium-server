/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.profiles;

import io.debezium.server.redis.lifecyclemanagers.RedisTestResourceLifecycleManager;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

import java.util.List;
import java.util.Map;

public class RedisStreamMemoryThresholdTestProfile extends RedisStreamTestProfile {
    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(new TestResourceEntry(RedisTestResourceLifecycleManager.class),
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = super.getConfigOverrides();
        config.put("debezium.sink.redis.memory.threshold.percentage", "75");
        return config;
    }
}
