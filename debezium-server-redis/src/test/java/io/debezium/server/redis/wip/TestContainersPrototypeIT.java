/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestUtils.awaitStreamLength;
import static io.debezium.server.redis.wip.TestUtils.getPostgresConnection;
import static io.debezium.server.redis.wip.TestUtils.insertCustomerToPostgres;
import static io.debezium.server.redis.wip.TestUtils.waitForContainerLog;
import static io.debezium.server.redis.wip.TestUtils.waitForContainerStop;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;

public class TestContainersPrototypeIT extends TestContainersRedisTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainersPrototypeIT.class);

    @Test
    public void shouldStreamChanges() throws InterruptedException, IOException {
        server.setEnv(new DebeziumServerConfigBuilder().withBaseConfig(redis, postgres).build());
        server.start();

        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);
        assertThat(server.getStandardOutput()).contains("inventory.customers");

        insertCustomerToPostgres(postgres, "Sergei", "Savage", "sesa@email.com");

        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT + 1);
    }

    @Test
    public void shouldFailWithIncorrectRedisAddress() {
        server.setEnv(new DebeziumServerConfigBuilder()
                .withBaseConfig(redis, postgres)
                .withValue("debezium.sink.redis.address", redis.getContainerIp() + ":" + 1000)
                .build());
        server.start();

        waitForContainerLog(server, "Failed to connect to any host resolved for DNS name");
        waitForContainerStop(server);
    }

    @Test
    @FixFor("DBZ-4510")
    public void testRedisConnectionRetry() throws Exception {
        final int SOCKET_TIMEOUT = 4000;
        server.setEnv(new DebeziumServerConfigBuilder().withBaseConfig(redis, postgres)
                .withValue("debezium.sink.redis.socket.timeout.ms", String.valueOf(SOCKET_TIMEOUT))
                .build());
        server.start();

        final int MESSAGE_COUNT = 5;
        final String STREAM_NAME = "testc.inventory.redis_test";
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

        LOGGER.info("Sleeping for " + SOCKET_TIMEOUT / 2 + " milis to simulate no connection errors");
        Thread.sleep(SOCKET_TIMEOUT / 2);
        redis.unpause();
        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);
    }

    @Test
    @FixFor("DBZ-4510")
    public void testRedisOOMRetry() throws Exception {
        server.setEnv(new DebeziumServerConfigBuilder().withBaseConfig(redis, postgres).build());
        server.start();

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

        waitForContainerLog(server, "Sink memory threshold percentage was reached");
        assertThat(jedis.xlen(STREAM_NAME)).isLessThan(TOTAL_RECORDS);

        LOGGER.info("Unsetting Redis' maxmemory");
        jedis.configSet("maxmemory", "30M");
        awaitStreamLength(jedis, STREAM_NAME, TOTAL_RECORDS);
    }
}
