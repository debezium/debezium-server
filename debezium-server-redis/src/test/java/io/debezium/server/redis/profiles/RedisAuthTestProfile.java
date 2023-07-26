package io.debezium.server.redis.profiles;

import io.debezium.server.redis.lifecyclemanagers.RedisAuthTestLifecycleManager;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Arrays;
import java.util.List;

public class RedisAuthTestProfile implements QuarkusTestProfile {
    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(new TestResourceEntry(RedisAuthTestLifecycleManager.class),
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class));
    }
}
