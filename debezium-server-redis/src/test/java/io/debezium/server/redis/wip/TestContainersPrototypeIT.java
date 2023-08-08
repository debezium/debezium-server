/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.POSTGRES_DATABASE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_IMAGE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PORT;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_USER;
import static io.debezium.server.redis.wip.TestConstants.REDIS_IMAGE;
import static io.debezium.server.redis.wip.TestConstants.REDIS_PORT;
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_SERVER_IMAGE_GROUP;
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import io.debezium.server.redis.TestUtils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersPrototypeIT {

    private final TestContainersResource postgres = new TestContainersResource(POSTGRES_IMAGE, POSTGRES_PORT,
            List.of("POSTGRES_USER=" + POSTGRES_USER,
                    "POSTGRES_PASSWORD=" + POSTGRES_PASSWORD,
                    "POSTGRES_DB=" + POSTGRES_DATABASE,
                    "POSTGRES_INITDB_ARGS=\"-E UTF8\"",
                    "LANG=en_US.utf8"));

    private final TestContainersResource redis = new TestContainersResource(REDIS_IMAGE, REDIS_PORT, List.of());

    @BeforeEach
    public void setUp() {
        postgres.start();
        redis.start();
    }

    @AfterEach
    public void tearDown() {
        postgres.stop();
        redis.stop();
    }

    @Test
    public void runServerContainer() throws InterruptedException, IOException {
        TestContainersResource server = new TestContainersResource(DEBEZIUM_SERVER_IMAGE_GROUP + "/debezium-server-redis:" + DEBEZIUM_VERSION, 8080,
                List.of("debezium.sink.type=redis",
                        "debezium.sink.redis.address=" + getContainerIp(redis.getContainer()) + ":" + REDIS_PORT,
                        "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector",
                        "debezium.source.offset.flush.interval.ms=0",
                        "debezium.source.topic.prefix=testc",
                        "debezium.source.schema.include.list=inventory",
                        "debezium.source.database.hostname=" + getContainerIp(postgres.getContainer()),
                        "debezium.source.database.port=" + POSTGRES_PORT,
                        "debezium.source.database.user=" + POSTGRES_USER,
                        "debezium.source.database.password=" + POSTGRES_PASSWORD,
                        "debezium.source.database.dbname=" + POSTGRES_DATABASE,
                        "debezium.source.offset.storage.file.filename=" + "offset.dat"));

        server.start();
        Jedis jedis = new Jedis(HostAndPort.from(getRedisContainerAddress(redis)));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);
        assertThat(server.getStandardOutput()).contains("inventory.customers");

        postgres.getContainer().execInContainer("psql",
                "-U", POSTGRES_USER,
                "-d", POSTGRES_DATABASE,
                "-c", "INSERT INTO inventory.customers VALUES (1005, 'aaa','aaaa','aa@example.com')");

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT + 1);
        System.out.println(server.getStandardOutput());
        server.stop();
    }

    @Test
    public void shouldFailWithIncorrectRedisAddress() {
        TestContainersResource server = new TestContainersResource(DEBEZIUM_SERVER_IMAGE_GROUP + "/debezium-server-redis:" + DEBEZIUM_VERSION, 8080,
                List.of("debezium.sink.type=redis",
                        "debezium.sink.redis.address=" + getContainerIp(redis.getContainer()) + ":" + 1000, // Incorrect port
                        "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector",
                        "debezium.source.offset.flush.interval.ms=0",
                        "debezium.source.topic.prefix=testc",
                        "debezium.source.schema.include.list=inventory",
                        "debezium.source.database.hostname=" + getContainerIp(postgres.getContainer()),
                        "debezium.source.database.port=" + POSTGRES_PORT,
                        "debezium.source.database.user=" + POSTGRES_USER,
                        "debezium.source.database.password=" + POSTGRES_PASSWORD,
                        "debezium.source.database.dbname=" + POSTGRES_DATABASE,
                        "debezium.source.offset.storage.file.filename=" + "offset.dat"));

        server.start();
        assertThat(server.getStandardOutput()).contains("Failed to connect to any host resolved for DNS name");
        server.stop();
    }

    private static String getContainerIp(GenericContainer<?> container) {
        return container
                .getContainerInfo()
                .getNetworkSettings()
                .getNetworks()
                .entrySet()
                .stream()
                .findFirst()
                .get()
                .getValue()
                .getIpAddress();
    }

    public static String getRedisContainerAddress(TestContainersResource resource) {
        return String.format("%s:%d", getContainerIp(resource.getContainer()), resource.getPort());
    }
}
