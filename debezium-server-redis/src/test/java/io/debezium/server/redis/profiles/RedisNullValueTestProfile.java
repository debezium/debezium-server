package io.debezium.server.redis.profiles;

import io.debezium.server.redis.lifecyclemanagers.RedisTestResourceLifecycleManager;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisNullValueTestProfile implements QuarkusTestProfile {
    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();
    public static final String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";

    public static final String NULL_VALUE_REPLACEMENT = "FooBar";
    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(RedisTestResourceLifecycleManager.class));
    }
    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        config.put("debezium.sink.redis.null.value", NULL_VALUE_REPLACEMENT);
        config.put("debezium.source." + OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        return config;
    }
}
