package io.debezium.server.redis.lifecyclemanagers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import io.debezium.server.TestConfigSource;
import io.debezium.server.redis.TestUtils;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import lombok.SneakyThrows;

public class RedisAuthTestLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final int REDIS_PORT = 6379;
    public static final String REDIS_IMAGE = "redis";
    static final String READY_MESSAGE = "Ready to accept connections";
    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();
    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static final GenericContainer<?> container = new GenericContainer<>(REDIS_IMAGE)
            .withClasspathResourceMapping("ssl", "/etc/certificates", BindMode.READ_ONLY)
            .withExposedPorts(REDIS_PORT);

    private static synchronized void start(boolean ignored) throws IOException, InterruptedException {
        if (!running.get()) {
            container.start();
            TestUtils.waitBoolean(() -> container.getLogs().contains(READY_MESSAGE));
            container.execInContainer("redis-cli", "ACL", "SETUSER", "debezium", ">dbz", "on", "+@all", "~*", "&*");
            container.execInContainer("redis-cli", "ACL", "SETUSER", "default", "off");

            running.set(true);
        }
    }

    @SneakyThrows
    @Override
    public Map<String, String> start() {
        start(true);
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(TestConfigSource.OFFSET_STORE_PATH);

        return Map.of("debezium.sink.redis.address", RedisAuthTestLifecycleManager.getRedisContainerAddress());
    }

    @Override
    public void stop() {
        try {
            container.stop();
        }
        catch (Exception e) {
            // ignored
        }
        running.set(false);
    }

    public static String getRedisContainerAddress() {
        // start(true);
        return String.format("%s:%d", container.getContainerIpAddress(), container.getMappedPort(REDIS_PORT));
    }
}
