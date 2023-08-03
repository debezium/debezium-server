package io.debezium.server.redis.profiles;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.debezium.server.TestConfigSource;
import io.debezium.server.redis.lifecyclemanagers.RedisAuthTestLifecycleManager;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisInvalidAuthTestProfile implements QuarkusTestProfile {
    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(new TestResourceEntry(RedisAuthTestLifecycleManager.class),
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        var params = new ConcurrentHashMap<String, String>();
        params.put("debezium.sink.type", "redis");
        params.put("debezium.sink.redis.user", "debezium");

        params.put("debezium.sink.redis.password", "forgottenPass");

        params.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        params.put("debezium.source.offset.flush.interval.ms", "0");
        params.put("debezium.source.topic.prefix", "testc");
        params.put("debezium.source.schema.include.list", "inventory");
        params.put("debezium.source.table.include.list", "inventory.customers,inventory.redis_test,inventory.redis_test2");
        params.put("debezium.source.offset.storage.file.filename", TestConfigSource.OFFSET_STORE_PATH.toString());
        return params;
    }
}
