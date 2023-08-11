/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestUtils.awaitStreamLength;
import static io.debezium.server.redis.wip.TestUtils.getContainerIp;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;

import io.debezium.server.redis.TestUtils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersSslStreamIT extends TestContainersRedisTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainersSslStreamIT.class);

    public TestContainersSslStreamIT() {
        super();
        redis
                .withCommand(
                        "redis-server --tls-port 6379 " +
                                "--port 6378 " +
                                "--tls-cert-file /etc/certificates/redis.crt " +
                                "--tls-key-file /etc/certificates/redis.key " +
                                "--tls-ca-cert-file /etc/certificates/ca.crt")
                .withClasspathResourceMapping("ssl", "/etc/certificates", BindMode.READ_ONLY);
        // server
        // TODO why is withCommand not working
        // .withCommand("-Djavax.net.ssl.keyStore=/ssl/client-keystore.p12",
        // "-Djavax.net.ssl.trustStore=/ssl/client-truststore.p12",
        // "-Djavax.net.ssl.keyStorePassword=secret",
        // "-Djavax.net.ssl.trustStorePassword=secret")
        // .withClasspathResourceMapping("ssl", "/etc/certificates", BindMode.READ_WRITE);
    }

    @Test
    public void testRedisSslStream() {
        prepareServerEnv();
        server.start();

        Jedis jedis = new Jedis(new HostAndPort(getContainerIp(redis), 6378));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";
        final String HASH_NAME = "metadata:debezium:offsets";

        awaitStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);

        long streamLength = jedis.xlen(STREAM_NAME);
        assertThat(streamLength).isEqualTo(MESSAGE_COUNT);

        // wait until the offsets are re-written
        TestUtils.awaitHashSizeGte(jedis, HASH_NAME, 1);

        Map<String, String> redisOffsets = jedis.hgetAll(HASH_NAME);
        assertThat(redisOffsets.size()).isPositive();

        jedis.close();
    }

    private void prepareServerEnv() {
        if (!redis.isRunning() || !postgres.isRunning()) {
            throw new IllegalStateException("Cannot prepare server environment without redis and postgres running");
        }

        server.setEnv(new DebeziumServerConfigBuilder()
                .withBaseConfig(redis, postgres)
                .withValue("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore")
                .withValue("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .withValue("debezium.source.offset.storage.redis.ssl.enabled", "true")
                .withValue("debezium.sink.redis.ssl.enabled", "true")
                .build());
    }
}
