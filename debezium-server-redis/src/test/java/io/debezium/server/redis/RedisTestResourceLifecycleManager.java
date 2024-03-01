/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import io.debezium.server.TestConfigSource;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class RedisTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTestResourceLifecycleManager.class);

    static final String READY_MESSAGE = "Ready to accept connections";
    public static final int REDIS_PORT = 6379;
    public static final int HOST_PORT = 16379;
    public static final String REDIS_IMAGE = "redis";

    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static final FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(REDIS_IMAGE)
            .withFixedExposedPort(HOST_PORT, REDIS_PORT);

    private static synchronized void start(boolean ignored) {
        if (!running.get()) {
            container.start();
            TestUtils.waitBoolean(() -> container.getLogs().contains(READY_MESSAGE));
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        start(true);
        container.followOutput(new Slf4jLogConsumer(LOGGER));
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(TestConfigSource.OFFSET_STORE_PATH);

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.type", "redis");
        params.put("debezium.sink.redis.address", RedisTestResourceLifecycleManager.getRedisContainerAddress());
        params.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        params.put("debezium.source.offset.flush.interval.ms", "0");
        params.put("debezium.source.topic.prefix", "testc");
        params.put("debezium.source.schema.include.list", "inventory");
        params.put("debezium.source.table.include.list", "inventory.customers,inventory.redis_test,inventory.redis_test2");
        params.put("debezium.transforms", "addheader");
        params.put("debezium.transforms.addheader.type", "org.apache.kafka.connect.transforms.InsertHeader");
        params.put("debezium.transforms.addheader.header", "headerKey");
        params.put("debezium.transforms.addheader.value.literal", "headerValue");

        return params;
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

    public static void pause() {
        container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
    }

    public static void unpause() {
        container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
    }

    public static String getRedisContainerAddress() {
        start(true);

        return String.format("%s:%d", container.getContainerIpAddress(), container.getMappedPort(REDIS_PORT));
    }
}
