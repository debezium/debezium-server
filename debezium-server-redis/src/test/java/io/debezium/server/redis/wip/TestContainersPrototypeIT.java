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
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_SERVER_IMAGE;
import static io.debezium.server.redis.wip.TestUtils.awaitStreamLength;
import static io.debezium.server.redis.wip.TestUtils.awaitStreamLengthGte;
import static io.debezium.server.redis.wip.TestUtils.getPostgresConnection;
import static io.debezium.server.redis.wip.TestUtils.getRedisContainerAddress;
import static io.debezium.server.redis.wip.TestUtils.insertCustomerToPostgres;
import static io.debezium.server.redis.wip.TestUtils.waitForContainerLog;
import static io.debezium.server.redis.wip.TestUtils.waitForContainerStop;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersPrototypeIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainersPrototypeIT.class);

    private final TestContainersResource postgres = TestContainersResource.builder()
            .withName("postgres")
            .withImage(POSTGRES_IMAGE)
            .withPort(POSTGRES_PORT)
            .withEnv(List.of("POSTGRES_USER=" + POSTGRES_USER,
                    "POSTGRES_PASSWORD=" + POSTGRES_PASSWORD,
                    "POSTGRES_DB=" + POSTGRES_DATABASE,
                    "POSTGRES_INITDB_ARGS=\"-E UTF8\"",
                    "LANG=en_US.utf8"))
            .build();

    private final TestContainersResource redis = TestContainersResource.builder()
            .withName("redis")
            .withImage(REDIS_IMAGE)
            .withPort(REDIS_PORT)
            .build();

    private final TestContainersResource server = TestContainersResource.builder()
            .withName("debezium server")
            .withImage(DEBEZIUM_SERVER_IMAGE)
            .build();

    @BeforeEach
    public void setUp() {
        postgres.start();
        redis.start();
    }

    @AfterEach
    public void tearDown() {
        server.stop();
        postgres.stop();
        redis.stop();
    }

    @Test
    public void shouldStreamChanges() throws InterruptedException, IOException {
        server.setEnv(new DebeziumServerConfigBuilder().withBaseConfig(redis, postgres).build());
        server.start();
        Jedis jedis = new Jedis(HostAndPort.from(getRedisContainerAddress(redis)));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);
        assertThat(server.getStandardOutput()).contains("inventory.customers");

        insertCustomerToPostgres(postgres.getContainer(), "Sergei", "Savage", "sesa@email.com");

        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT + 1);
    }

    @Test
    public void shouldFailWithIncorrectRedisAddress() {
        server.setEnv(new DebeziumServerConfigBuilder()
                .withBaseConfig(redis, postgres)
                .withValue("debezium.sink.redis.address", redis.getContainerIp() + ":" + 1000)
                .build());

        server.start();
        waitForContainerLog(server.getContainer(), "Failed to connect to any host resolved for DNS name");
        waitForContainerStop(server.getContainer());
    }

    @Test
    @FixFor("DBZ-4510")
    public void testRedisConnectionRetry() throws Exception {
        server.setEnv(new DebeziumServerConfigBuilder().withBaseConfig(redis, postgres).build());
        server.start();

        final int MESSAGE_COUNT = 5;
        final String STREAM_NAME = "testc.inventory.redis_test";
        Jedis jedis = new Jedis(HostAndPort.from(getRedisContainerAddress(redis)));
        redis.pause();

        final PostgresConnection connection = getPostgresConnection(postgres);
        LOGGER.info("Creating new redis_test table and inserting 5 records to it");
        connection.execute(
                "CREATE TABLE inventory.redis_test (id INT PRIMARY KEY)",
                "INSERT INTO inventory.redis_test VALUES (1)",
                "INSERT INTO inventory.redis_test VALUES (2)",
                "INSERT INTO inventory.redis_test VALUES (3)",
                "INSERT INTO inventory.redis_test VALUES (4)",
                "INSERT INTO inventory.redis_test VALUES (5)");
        connection.close();

        LOGGER.info("Sleeping for 3 seconds to simulate no connection errors");
        Thread.sleep(3000);
        redis.resume();

        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);
    }

    @Test
    @FixFor("DBZ-4510")
    public void testRedisOOMRetry() throws Exception {
        server.setEnv(new DebeziumServerConfigBuilder().withBaseConfig(redis, postgres).build());
        server.start();

        Jedis jedis = new Jedis(HostAndPort.from(getRedisContainerAddress(redis)));
        final String STREAM_NAME = "testc.inventory.redis_test2";
        final int TOTAL_RECORDS = 50;

        LOGGER.info("Setting Redis' maxmemory to 1M");
        jedis.configSet("maxmemory", "1M");

        PostgresConnection connection = getPostgresConnection(postgres);
        connection.execute("CREATE TABLE inventory.redis_test2 " +
                "(id VARCHAR(100) PRIMARY KEY, " +
                "first_name VARCHAR(100), " +
                "last_name VARCHAR(100))");
        connection.execute(String.format("INSERT INTO inventory.redis_test2 (id,first_name,last_name) " +
                "SELECT LEFT(i::text, 10), RANDOM()::text, RANDOM()::text FROM generate_series(1,%d) s(i)", TOTAL_RECORDS));
        connection.commit();

        // wait for debezium to stream changes to redis and hit oom
        awaitStreamLengthGte(jedis, STREAM_NAME, 0);
        Thread.sleep(1000);
        LOGGER.info("Entries in " + STREAM_NAME + ":" + jedis.xlen(STREAM_NAME));
        assertThat(jedis.xlen(STREAM_NAME)).isLessThan(TOTAL_RECORDS);

        Thread.sleep(1000);
        LOGGER.info("Unsetting Redis' maxmemory");
        jedis.configSet("maxmemory", "30M");
        awaitStreamLength(jedis, STREAM_NAME, TOTAL_RECORDS);
    }
}
