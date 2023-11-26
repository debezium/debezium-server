/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static io.debezium.server.redis.TestConstants.LOCALHOST;
import static io.debezium.server.redis.TestConstants.MYSQL_DATABASE;
import static io.debezium.server.redis.TestConstants.MYSQL_PORT;
import static io.debezium.server.redis.TestConstants.MYSQL_PRIVILEGED_PASSWORD;
import static io.debezium.server.redis.TestConstants.MYSQL_PRIVILEGED_USER;
import static io.debezium.server.redis.TestConstants.MYSQL_ROOT_PASSWORD;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;

import redis.clients.jedis.Jedis;

public class TestUtils {

    public static void insertCustomerToMySql(DebeziumTestContainerWrapper container, String firstName, String lastName, String email)
            throws IOException, InterruptedException {
        container.execInContainer("mysql",
                "-u", MYSQL_PRIVILEGED_USER,
                "-p" + MYSQL_PRIVILEGED_PASSWORD,
                "-D", MYSQL_DATABASE,
                "-e", "INSERT INTO customers VALUES (default,'" + firstName + "','" + lastName + "','" + email + "')");
    }

    public static MySqlConnection getMySqlConnection(DebeziumTestContainerWrapper container) {
        return new MySqlConnection(new MySqlConnection.MySqlConnectionConfiguration(Configuration.create()
                .with("database.user", "root")
                .with("database.password", MYSQL_ROOT_PASSWORD)
                .with("database.dbname", MYSQL_DATABASE)
                .with("database.hostname", LOCALHOST)
                .with("database.port", container.getMappedPort(MYSQL_PORT))
                .build()));
    }

    public static void waitForStreamLength(Jedis jedis, String streamName, int expectedLength) {
        await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> jedis.xlen(streamName) == expectedLength);
    }

    public static void awaitHashSizeGte(Jedis jedis, String hashName, int expectedSize) {
        await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> jedis.hgetAll(hashName).size() >= expectedSize);
    }

}
