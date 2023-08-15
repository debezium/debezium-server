/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.INITIAL_CUSTOMER_COUNT;
import static io.debezium.server.redis.wip.TestConstants.INITIAL_SCHEMA_HISTORY_SIZE;
import static io.debezium.server.redis.wip.TestConstants.LOCALHOST;
import static io.debezium.server.redis.wip.TestConstants.REDIS_PORT;
import static io.debezium.server.redis.wip.TestUtils.insertCustomerToMySql;
import static io.debezium.server.redis.wip.TestUtils.waitForStreamLength;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;

import io.debezium.server.redis.TestUtils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersSslStreamIT extends TestContainersRedisTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainersSslStreamIT.class);
    private static final int REDIS_NON_SSL_PORT = 6378;

    public TestContainersSslStreamIT() {
        super();
        redis
                .withCommand(
                        "redis-server --tls-port " + REDIS_PORT + " " +
                                "--port " + REDIS_NON_SSL_PORT + " " +
                                "--tls-cert-file /etc/certificates/redis.crt " +
                                "--tls-key-file /etc/certificates/redis.key " +
                                "--tls-ca-cert-file /etc/certificates/ca.crt")
                .withExposedPorts(REDIS_NON_SSL_PORT, REDIS_PORT)
                .withClasspathResourceMapping("ssl", "/etc/certificates", BindMode.READ_ONLY);
        server
                .withCommand("-Djavax.net.ssl.keyStore=/ssl/client-keystore.p12",
                        "-Djavax.net.ssl.trustStore=/ssl/client-truststore.p12",
                        "-Djavax.net.ssl.keyStorePassword=secret",
                        "-Djavax.net.ssl.trustStorePassword=secret",
                        "-jar", "quarkus-run.jar")
                .withClasspathResourceMapping("ssl", "/ssl", BindMode.READ_WRITE);
    }

    @Test
    public void shouldStreamWithSslEnabled() throws IOException, InterruptedException {
        server.setEnv(new DebeziumServerConfigBuilder().withBaseMySqlConfig(redis, mysql)
                .withValue("debezium.sink.redis.ssl.enabled", "true")
                .withValue("debezium.source.database.ssl.mode", "disabled")
                .withValue("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore")
                .build());
        server.start();

        jedis = new Jedis(new HostAndPort(LOCALHOST, redis.getMappedPort(REDIS_NON_SSL_PORT)));
        final String STREAM_NAME = "testc.inventory.customers";

        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT);

        insertCustomerToMySql(mysql, "Sergei", "Savage", "sesa@email.com");
        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT + 1);

        TestUtils.awaitHashSizeGte(jedis, "metadata:debezium:offsets", 1);
        waitForStreamLength(jedis, "metadata:debezium:schema_history", INITIAL_SCHEMA_HISTORY_SIZE);
    }
}
