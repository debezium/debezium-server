/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.POSTGRES_DATABASE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PORT;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_USER;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.server.TestConfigSource;

import redis.clients.jedis.Jedis;

public class TestUtils {
    public static void waitForContainerLog(GenericContainer<?> container, String expectedLog) {
        await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> container.getLogs(OutputFrame.OutputType.STDOUT).contains(expectedLog));
    }

    public static void waitForContainerStop(GenericContainer<?> container) {
        await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> !container.isRunning());
    }

    static String getContainerIp(GenericContainer<?> container) {
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

    public static String getRedisContainerAddress(DebeziumTestContainerWrapper resource) {
        return String.format("%s:%d", getContainerIp(resource), resource.getExposedPorts().get(0));
    }

    public static void insertCustomerToPostgres(DebeziumTestContainerWrapper container, String firstName, String lastName, String email)
            throws IOException, InterruptedException {
        container.execInContainer("psql",
                "-U", POSTGRES_USER,
                "-d", POSTGRES_DATABASE,
                "-c", "INSERT INTO inventory.customers VALUES (default,'" + firstName + "','" + lastName + "','" + email + "')");
    }

    public static PostgresConnection getPostgresConnection(DebeziumTestContainerWrapper container) {
        return new PostgresConnection(JdbcConfiguration.create()
                .with("user", POSTGRES_USER)
                .with("password", POSTGRES_PASSWORD)
                .with("dbname", POSTGRES_DATABASE)
                .with("hostname", container.getContainerIp())
                .with("port", POSTGRES_PORT)
                .build(), "Debezium Redis Test");
    }

    public static void awaitStreamLengthGte(Jedis jedis, String streamName, int expectedLength) {
        await()
                .atMost(TestConfigSource.waitForSeconds(), TimeUnit.SECONDS)
                .until(() -> jedis.xlen(streamName) >= expectedLength);

    }

    public static void awaitStreamLength(Jedis jedis, String streamName, int expectedLength) {
        await()
                .atMost(TestConfigSource.waitForSeconds(), TimeUnit.SECONDS)
                .until(() -> jedis.xlen(streamName) == expectedLength);
    }

}
